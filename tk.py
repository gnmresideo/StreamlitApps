import streamlit as st
import re
import base64
#import snowflake.snowpark   
#from snowflake.snowpark 
import Session

def encode_file_to_base64(uploaded_file):
    """Convert an uploaded file to a base64 string."""
    if uploaded_file:
        return base64.b64encode(uploaded_file.getvalue()).decode("utf-8")
    return None

def insert_ticket(session, function, email, request_type, request_title, request_name, encoded_file):
    """
    Insert ticket details into Snowflake using dynamic SQL.
    This version also sets Region, Business_Segment, and Download based on rules:
      - If function is in group2 (Snap ...), then Region = "NA" and Business_Segment = "Snap One"
      - If the first 4 characters (case-insensitive) are "EMEA", then both Region and Business_Segment are "EMEA"
      - If the first 2 characters (case-insensitive) are "NA", then Region = "NA" and Business_Segment = "Business Support Group"
      - Otherwise, default to Region = "NA" and Business_Segment = "Business Support Group"
      - If a file is uploaded, Download is set to CHR(9660) ("▼"); otherwise, it's NULL.
    """

    def esc(s):
        """Escape single quotes to avoid SQL injection."""
        return s.replace("'", "''") if s else ""
    
    # Escape user inputs
    safe_function = esc(function)
    safe_email = esc(email)
    safe_rtype = esc(request_type)
    safe_rtitle = esc(request_title)
    safe_rname = esc(request_name)
    
    # Data_Manager logic (unchanged)
    group1 = [
        "Branch", "Category Management", "Credit", "Customer Service",
        "Data Analytics", "Data Comm", "DX", "Inventory", "Other",
        "Outbound Telesales", "Pro AV", "RAS/NAM"
    ]
    group2 = [
        "Snap Accounting", "Snap DX", "Snap Manufacturing & Quality",
        "Snap Operations", "Snap Rewards & Marketing", "Snap Sales",
        "Snap Support & Education"
    ]
    if function in group1:
        data_manager = "ana-mari.pita@adiglobal.com; john.larosa@adiglobal.com"
    elif function in group2:
        data_manager = "dale.slaughenhaupt@adiglobal.com"
    else:
        data_manager = ""
    safe_data_manager = esc(data_manager)
    
    # Determine Region and Business_Segment based solely on the function
    fn_upper = function.upper()
    if function in group2:
        business_segment = "Snap One"
        region = "NA"
    elif fn_upper.startswith("EMEA"):
        business_segment = "EMEA"
        region = "EMEA"
    elif fn_upper.startswith("NA"):
        business_segment = "Business Support Group"
        region = "NA"
    else:
        # Default for any other function:
        business_segment = "Business Support Group"
        region = "NA"

    region_expr = f"'{region}'"
    bs_expr = f"'{business_segment}'"

    # Upload column: store the base64 string if file attached
    upload_expr = "NULL"
    if encoded_file:
        safe_b64 = encoded_file.replace("'", "''")
        upload_expr = f"'{safe_b64}'"

    # Download column: if a file is attached, write the down arrow; else NULL.
    if encoded_file:
        download_val = f"'{chr(9660)}'"
    else:
        download_val = "NULL"

    sql = f"""
    INSERT INTO DB_ADI_NA_PROD.SCH_ADI_NA_ENDUSER_REPORTS.ADI_TICKET_SYSTEM
    (
        Function_Name,
        Requestor_Email,
        Request_Type,
        Request_Title,
        Request_Name,
        Data_Manager,
        Upload,
        Region,
        Business_Segment,
        Download
    )
    VALUES (
        '{safe_function}',
        '{safe_email}',
        '{safe_rtype}',
        '{safe_rtitle}',
        '{safe_rname}',
        '{safe_data_manager}',
        {upload_expr},
        {region_expr},
        {bs_expr},
        {download_val}
    )
    """
    session.sql(sql).collect()

def main():
    st.set_page_config(page_title="Analytics Ticket System", page_icon="📊", layout="centered")

    # Initialize session state for ticket submission
    if "ticket_submitted" not in st.session_state:
        st.session_state.ticket_submitted = False

    # ------------------ CUSTOM CSS FOR THE FORM ------------------ #
    st.markdown(
        """
        <style>
            .stApp {
                background-color: #0c1c2c;
            }
            .stForm {
                background-color: white;
                padding: 20px;
                box-shadow: 0px 0px 10px rgba(255, 255, 255, 0.1);
                width: 80%;
                margin: auto;
            }
            .field-label {
                color: black;
                font-weight: bold;
                font-size: 18px;
                margin-bottom: -30px;
                display: block;
            }
            .stTextInput>div, .stTextArea>div, .stSelectbox>div, .stFileUploader>div {
                margin-top: -8px !important;
            }
            .stFileUploader label {
                margin-bottom: -5px !important;
                margin-top: -15px;
            }
            .stTextInput, .stTextArea, .stSelectbox, .stFileUploader, .stButton {
                border-radius: 5px;
                background-color: white !important;
                color: black !important;
            }
        </style>
        """,
        unsafe_allow_html=True
    )
    # ------------------------------------------------------------- #

    # Header
    st.markdown("""
    <h1 style='text-align: center; color: white;'>Analytics Ticket System</h1>
    <p style='text-align: center; color: white;'>
      Welcome to the ADI Analytics ticket system. Please do your best to provide as much detail as possible in your requests.
    </p>
    """, unsafe_allow_html=True)

    # Define the success message to display after submission
    success_message = """
    <div style="background-color: white; 
                border-radius: 5px; 
                padding: 20px; 
                margin: 50px auto; 
                width: 80%; 
                text-align: center;">
      <p style="color: green; 
                font-size: 18px; 
                font-weight: bold;">
        Your ticket has been successfully submitted.<br>
        Please refresh the page if you would like to submit another ticket.
      </p>
    </div>
    """

    # Create an empty container to hold the form
    form_container = st.empty()

    # If a ticket has already been submitted, display the success message and skip the form
    if st.session_state.ticket_submitted:
        form_container.empty()
        st.markdown(success_message, unsafe_allow_html=True)
        return

    # Create Snowflake session
    #session = Session.builder.getOrCreate()

#    @st.cache_resource
#    def create_session():
#        return Session.builder.configs(st.secrets.snowflake).create()
#    session = create_session()
    def get_snowflake_session():
        return Session.builder.configs(st.secrets.snowflake).getOrCreate()

    with form_container.form("ticket_form"):
        st.markdown("<div class='stForm'>", unsafe_allow_html=True)

        # Function Dropdown
        st.markdown("<span class='field-label'>What is your function?</span>", unsafe_allow_html=True)
        function = st.selectbox(
            "",
            [
                "Branch", "Category Management", "Credit", "Customer Service",
                "Data Analytics", "Data Comm", "DX", "Inventory", "Other", "Outbound Telesales",
                "Pro AV", "RAS/NAM", "Snap Accounting", "Snap DX", "Snap Manufacturing & Quality",
                "Snap Operations", "Snap Rewards & Marketing", "Snap Sales", "Snap Support & Education"
            ],
            index=None,
            key="function",
            placeholder="Select an Option"
        )

        # Email Input
        st.markdown("<span class='field-label'>What is your Email?</span>", unsafe_allow_html=True)
        email = st.text_input("", value="", key="email")

        # Request Type
        st.markdown("<span class='field-label'>Request Type:</span>", unsafe_allow_html=True)
        request_type = st.selectbox(
            "",
            ["Report Request", "BI Tool Inquiry", "BI Tool Request", "Adhoc/Automated"],
            index=None,
            key="request_type",
            placeholder="Select an Option"
        )

        # Request Title
        st.markdown("<span class='field-label'>Request Title</span>", unsafe_allow_html=True)
        request_title = st.text_input("", value="", key="request_title")

        # Request Name
        st.markdown("<span class='field-label'>Request Name</span>", unsafe_allow_html=True)
        request_name = st.text_area("", value="", key="request_name")

        # File Upload
        uploaded_file = st.file_uploader(
            "",
            type=["xlsx", "xls"],
            accept_multiple_files=False,
            key="file_upload"
        )

        # Submit Button
        submit_button = st.form_submit_button("Submit")

        st.markdown("</div>", unsafe_allow_html=True)

    if submit_button:
        encoded_file = encode_file_to_base64(uploaded_file) if uploaded_file else None

        # Basic validations
        if not function or function.lower() == "select an option":
            st.error("Please select your function.")
        elif not email or not re.match(r"^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$", email):
            st.error("Please enter a valid email address.")
        elif not request_type or request_type.lower() == "select an option":
            st.error("Please select a request type.")
        elif not request_title.strip():
            st.error("Request Title cannot be empty.")
        elif not request_name.strip():
            st.error("Request Name cannot be empty.")
        elif uploaded_file and uploaded_file.size > 1_000_000:
            st.error("The file you submitted is too large. Please reduce the size of your file and try again.")
        else:
            try:
                # Insert ticket with new columns logic
                insert_ticket(session, function, email, request_type, request_title, request_name, encoded_file)
                st.session_state.ticket_submitted = True
                form_container.empty()  # Clear the form container
                st.markdown(success_message, unsafe_allow_html=True)
            except Exception as e:
                st.error(f"An error occurred: {e}")

if __name__ == "__main__":
    main()
