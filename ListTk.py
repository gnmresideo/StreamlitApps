import streamlit as st
import pandas as pd
import numpy as np
import base64
from datetime import date
from snowflake.snowpark import Session

######################
# 1) CONFIG & INIT
######################

def get_snowflake_session():
    return Session.builder.configs(st.secrets.snowflake).getOrCreate()

#-> Session:
#    """Initialize your Snowflake session.
#       Replace with your actual connection logic."""
#    return Session.builder.getOrCreate()
#@st.cache_resource
#def create_session():
#session = create_session()

    
# Filter options for the PM UI
REGION_OPTIONS = ["All", "NA", "EMEA"]
BUSINESS_SEGMENT_OPTIONS = ["All", "Snap One", "Business Support Group", "EMEA"]
FUNCTION_OPTIONS = [
    "Branch", "Category Management", "Credit", "Customer Service",
    "Data Analytics", "Data Comm", "DX", "Inventory", "Other", "Outbound Telesales",
    "Pro AV", "RAS/NAM", "Snap Accounting", "Snap DX", "Snap Manufacturing & Quality",
    "Snap Operations", "Snap Rewards & Marketing", "Snap Sales", "Snap Support & Education"
]
REQUEST_TYPE_OPTIONS = [
    "Report Request", "BI Tool Inquiry", "BI Tool Request", "Adhoc/Automated"
]
PROJECT_STATUS_OPTIONS = [
    "Not Assigned", "Assigned", "Pending", "Completed"
]

# Editable columns for PM (those the PM can modify)
# Note: "ASSIGNED" is no longer displayed, and "DATE_COMPLETED" is now read-only.
EDITABLE_COLS = [
    "DATE_COMPLETED",  # Even though it's read-only, we update it via cross-field logic.
    "FUNCTION_NAME", "REQUEST_TYPE", "PROJECT_STATUS",
    "ASSIGNED_NAME", "REGION", "BUSINESS_SEGMENT", "ETC", "COMMENTS"
]
# Read-only columns for PM (cannot be modified via the UI)
READ_ONLY_COLS = [
    "ID", "DATE_CREATED", "REQUEST_TITLE", "REQUEST_NAME", "DOWNLOAD", "DATE_COMPLETED"
]

######################
# 2) FETCH & FILTER
######################

def fetch_tickets(session: Session) -> pd.DataFrame:
    """
    Pull all rows from the table.
    """
    df = session.table("DB_ADI_NA_PROD.SCH_ADI_NA_ENDUSER_REPORTS.ADI_TICKET_SYSTEM")
    return df.to_pandas()

def apply_filters(df: pd.DataFrame, region_filter, bs_filter, function_filter, request_filter, status_filter) -> pd.DataFrame:
    filtered = df.copy()
    if region_filter != "All":
        filtered = filtered[filtered["REGION"] == region_filter]
    if bs_filter != "All":
        filtered = filtered[filtered["BUSINESS_SEGMENT"] == bs_filter]
    if function_filter != "All":
        filtered = filtered[filtered["FUNCTION_NAME"] == function_filter]
    if request_filter != "All":
        filtered = filtered[filtered["REQUEST_TYPE"] == request_filter]
    if status_filter != "All":
        filtered = filtered[filtered["PROJECT_STATUS"] == status_filter]
    return filtered

######################
# 3) PARTIAL UPDATE
######################

def update_cell(session: Session, row_id: int, col: str, new_val):
    """
    Partial UPDATE for a single cell. Minimal conversion is applied for date columns.
    """
    if col in ["DATE_COMPLETED", "ETC"]:
        if not new_val or pd.isna(new_val):
            set_expr = f"{col} = NULL"
        else:
            try:
                parsed = pd.to_datetime(str(new_val)).date()
                set_expr = f"{col} = '{parsed}'"
            except:
                st.warning(f"Invalid date '{new_val}' for {col}. Skipping update.")
                return
    else:
        val_str = str(new_val).replace("'", "''")
        set_expr = f"{col} = '{val_str}'"
    sql_update = f"""
        UPDATE DB_ADI_NA_PROD.SCH_ADI_NA_ENDUSER_REPORTS.ADI_TICKET_SYSTEM
        SET {set_expr}
        WHERE ID = {row_id}
    """
    try:
        session.sql(sql_update).collect()
    except Exception as e:
        st.error(f"Error updating row ID={row_id}, col={col}: {e}")

def apply_diffs_and_update(session: Session, old_df: pd.DataFrame, new_df: pd.DataFrame):
    """
    Compare old_df vs. new_df row by row/column by column.
    For each difference in an EDITABLE column, issue a partial update in Snowflake.
    Also perform cross-field updates:
      - Update ASSIGNED based on ASSIGNED_NAME (Y if non-empty, N if empty).
      - Update DATE_COMPLETED based on PROJECT_STATUS:
          * If "Completed", set to current date (yyyy-mm-dd).
          * Otherwise, set to NULL.
    """
    old_df = old_df.sort_values("ID").reset_index(drop=True)
    new_df = new_df.sort_values("ID").reset_index(drop=True)
    for idx in range(len(new_df)):
        row_id = new_df.loc[idx, "ID"]
        # First, update individual cell differences.
        for col in EDITABLE_COLS:
            if col not in new_df.columns:
                continue
            old_val = old_df.loc[idx, col]
            new_val = new_df.loc[idx, col]
            if (pd.isna(old_val) and pd.isna(new_val)):
                continue
            if old_val != new_val:
                update_cell(session, row_id, col, new_val)
        # Now, perform cross-field updates:
        # 1) Update ASSIGNED based on ASSIGNED_NAME:
        new_assigned_name = new_df.loc[idx, "ASSIGNED_NAME"]
        if pd.notna(new_assigned_name) and str(new_assigned_name).strip() != "":
            update_cell(session, row_id, "ASSIGNED", "Y")
        else:
            update_cell(session, row_id, "ASSIGNED", "N")
        # 2) Update DATE_COMPLETED based on PROJECT_STATUS:
        new_status = new_df.loc[idx, "PROJECT_STATUS"]
        if new_status == "Completed":
            today_str = str(date.today())
            update_cell(session, row_id, "DATE_COMPLETED", today_str)
        else:
            update_cell(session, row_id, "DATE_COMPLETED", None)

######################
# 4) MAIN APP
######################

def main():
    st.set_page_config(page_title="Project Manager UI", layout="wide")
    session = get_snowflake_session()

    st.title("Business Support RAIL")

    # 1) Custom CSS:
    st.markdown("""
    <style>
    /* Word-wrap for the COMMENTS column */
    [data-testid="stDataEditor"] table tbody td:last-child {
        white-space: normal !important;
        word-wrap: break-word !important;
    }
    /* Style the download button as blue */
    .stDownloadButton button {
        background-color: #0d6efd !important;
        color: white !important;
        border: none !important;
        border-radius: 0.25rem !important;
    }
    </style>
    """, unsafe_allow_html=True)

    # 2) Fetch data from DB
    df = fetch_tickets(session)

    # 3) Initialize session state for baseline and edited data if not already set
    if "old_df" not in st.session_state:
        st.session_state.old_df = df.copy()
    if "edited_df" not in st.session_state:
        st.session_state.edited_df = df.copy()

    # 4) Filter controls: Order = Region, Business Segment, Function, Request Type, Status, Apply button
    col1, col2, col3, col4, col5, col6 = st.columns([1,1,1,1,1,1])
    with col1:
        region_filter = st.selectbox("Filter by Region:", REGION_OPTIONS)
    with col2:
        bs_filter = st.selectbox("Filter by Business Segment:", BUSINESS_SEGMENT_OPTIONS)
    with col3:
        function_filter = st.selectbox("Filter by Function:", ["All"] + FUNCTION_OPTIONS)
    with col4:
        request_filter = st.selectbox("Filter by Request Type:", ["All"] + REQUEST_TYPE_OPTIONS)
    with col5:
        status_filter = st.selectbox("Filter by Status:", ["All"] + PROJECT_STATUS_OPTIONS)
    with col6:
        st.markdown("<div style='margin-top:1.8rem;'></div>", unsafe_allow_html=True)
        apply_button = st.button("Apply Changes", type="primary")

    st.info("Edit cells as needed. Click 'Apply Changes' above to commit all edits.")

    # 5) Filter the local edited_df for display
    filtered_df = apply_filters(st.session_state.edited_df, region_filter, bs_filter, function_filter, request_filter, status_filter)

    # 6) Define the columns in desired order.
    # Order: ID, DOWNLOAD, REGION, BUSINESS_SEGMENT, FUNCTION_NAME, REQUESTOR_EMAIL, DATE_CREATED, DATE_COMPLETED, ETC, REQUEST_TYPE, REQUEST_TITLE, REQUEST_NAME, ASSIGNED_NAME, PROJECT_STATUS, COMMENTS
    show_cols = [
        "ID", "DOWNLOAD", "REGION", "BUSINESS_SEGMENT", "FUNCTION_NAME", "REQUESTOR_EMAIL",
        "DATE_CREATED", "DATE_COMPLETED", "ETC", "REQUEST_TYPE", "REQUEST_TITLE", "REQUEST_NAME",
        "ASSIGNED_NAME", "PROJECT_STATUS", "COMMENTS"
    ]
    final_cols = [c for c in show_cols if c in filtered_df.columns]
    disabled_cols = ["ID", "DATE_CREATED", "REQUEST_TITLE", "REQUEST_NAME", "DOWNLOAD", "DATE_COMPLETED"]

    # 7) Data editor for the filtered local copy
    new_edited_df = st.data_editor(
        filtered_df[final_cols],
        num_rows="dynamic",
        hide_index=True,
        use_container_width=True,
        disabled=disabled_cols
    )

    # 8) When "Apply Changes" is clicked, merge changes and update DB
    if apply_button:
        def merge_filtered_edits(original: pd.DataFrame, filtered_old: pd.DataFrame, filtered_new: pd.DataFrame) -> pd.DataFrame:
            merged = original.copy()
            merged.set_index("ID", inplace=True)
            filtered_old = filtered_old.set_index("ID")
            filtered_new = filtered_new.set_index("ID")
            for idx in filtered_new.index:
                for col in filtered_new.columns:
                    if col not in merged.columns:
                        continue
                    old_val = filtered_old.loc[idx, col]
                    new_val = filtered_new.loc[idx, col]
                    if (pd.isna(old_val) and pd.isna(new_val)):
                        continue
                    if old_val != new_val:
                        merged.at[idx, col] = new_val
            merged.reset_index(inplace=True)
            return merged

        updated_full = merge_filtered_edits(st.session_state.edited_df, filtered_df, new_edited_df)
        st.session_state.edited_df = updated_full.copy()

        old_filtered = apply_filters(st.session_state.old_df, region_filter, bs_filter, function_filter, request_filter, status_filter)
        if len(new_edited_df) == len(old_filtered):
            apply_diffs_and_update(session, old_filtered, new_edited_df)
            fresh_df = fetch_tickets(session)
            st.session_state.old_df = fresh_df.copy()
            st.session_state.edited_df = fresh_df.copy()
            st.success("All changes applied to Snowflake. Data reloaded from DB.")
        else:
            st.warning("Shape mismatch. Possibly filters changed row counts, so no updates applied.")

    # 9) Download Section for files using the DOWNLOAD column
    download_df = st.session_state.edited_df[st.session_state.edited_df["DOWNLOAD"].notna()]
    if not download_df.empty:
        st.markdown("### Download Files")
        download_ids = download_df["ID"].tolist()
        selected_id = st.selectbox("Select Ticket ID to download file:", ["None"] + [str(x) for x in download_ids])
        if selected_id != "None":
            row = download_df[st.session_state.edited_df["ID"] == int(selected_id)]
            if not row.empty:
                # Retrieve file data from the hidden UPLOAD column
                upload_val = st.session_state.edited_df.loc[st.session_state.edited_df["ID"] == int(selected_id), "UPLOAD"]
                if not upload_val.empty:
                    base64_upload = upload_val.values[0]
                    if pd.notna(base64_upload) and base64_upload != "[NULL]":
                        try:
                            decoded_bytes = base64.b64decode(base64_upload)
                            st.download_button(
                                "Download File",
                                data=decoded_bytes,
                                file_name=f"ticket_{selected_id}.xlsx",
                                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                            )
                        except Exception as e:
                            st.error(f"Error decoding file: {e}")
                    else:
                        st.write("No File")
                else:
                    st.write("No File")

if __name__ == "__main__":
    main()
