import os
import psycopg2
from flask import Flask, request, jsonify
from simple_salesforce import Salesforce, SalesforceAuthenticationFailed, SalesforceResourceNotFound
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

app = Flask(__name__)

# --- Salesforce Configuration ---
SF_USERNAME = os.getenv('SALESFORCE_USERNAME')
SF_PASSWORD = os.getenv('SALESFORCE_PASSWORD')
SF_SECURITY_TOKEN = os.getenv('SALESFORCE_SECURITY_TOKEN')
SF_DOMAIN = os.getenv('SALESFORCE_DOMAIN', 'login') # Default to 'login' (production)

# --- PostgreSQL Configuration ---
PG_HOST = os.getenv('POSTGRES_HOST')
PG_DB = os.getenv('POSTGRES_DB')
PG_USER = os.getenv('POSTGRES_USER')
PG_PASSWORD = os.getenv('POSTGRES_PASSWORD')
PG_PORT = os.getenv('POSTGRES_PORT', 5432)


def get_db_connection():
    try:
        conn = psycopg2.connect(
            host=PG_HOST,
            database=PG_DB,
            user=PG_USER,
            password=PG_PASSWORD,
            port=PG_PORT
        )
        return conn
    except psycopg2.Error as e:
        app.logger.error(f"Error connecting to PostgreSQL: {e}")
        raise # Re-raise the exception to be caught by the route handler


def fetch_salesforce_data(sf_opportunity_discussed_id):
    """
    Fetches data from Salesforce for a given TR1__Opportunity_Discussed__c ID.
    """
    try:
        sf = Salesforce(
            username=SF_USERNAME,
            password=SF_PASSWORD,
            security_token=SF_SECURITY_TOKEN,
            domain=SF_DOMAIN
        )
        app.logger.info(f"Successfully connected to Salesforce (User: {SF_USERNAME}, Domain: {SF_DOMAIN})")
    except SalesforceAuthenticationFailed as e:
        app.logger.error(f"Salesforce authentication failed: {e}")
        raise
    except Exception as e:
        app.logger.error(f"Error initializing Salesforce connection: {e}")
        raise

    # Note: X2nd_Interveiw_transcript__c might be a typo for X2nd_Interview_transcript__c
    # Adjust the field name in the query if necessary based on your actual Salesforce schema.
    soql_query = f"""
        SELECT
            Id,
            TR1__Job__r.Id,
            TR1__Candidate__r.Id,
            TR1__Candidate__r.AccountId,
            TR1__Candidate__r.Candidate_s_Resume_TXT__c,
            TR1__Candidate__r.FirstName,
            Screening_Transcript__c,
            Internal_Interview_Transcript__c,
            X1st_Interview_Transcript__c            
        FROM
            TR1__Opportunity_Discussed__c
        WHERE Id = '{sf_opportunity_discussed_id}'
    """
    # Using X2nd_Interview_transcript__c (corrected typo) below as an example
    # soql_query = soql_query.replace("X2nd_Interveiw_transcript__c", "X2nd_Interview_transcript__c")
    
    app.logger.info(f"Executing SOQL query: {soql_query}")

    try:
        result = sf.query(soql_query)
        if result['totalSize'] > 0:
            app.logger.info(f"Data found in Salesforce for ID {sf_opportunity_discussed_id}")
            return result['records'][0]
        else:
            app.logger.warning(f"No data found in Salesforce for ID {sf_opportunity_discussed_id}")
            return None
    except SalesforceResourceNotFound:
        app.logger.warning(f"Salesforce resource not found for ID {sf_opportunity_discussed_id}")
        return None
    except Exception as e:
        app.logger.error(f"Error querying Salesforce: {e}")
        raise


def update_postgres_record(salesforce_opp_discussed_id, sf_data):
    """
    Updates a record in the od_data PostgreSQL table.
    The record is identified by the salesforce_opp_discussed_id.
    """
    # Map Salesforce fields to od_data table columns
    # Handle potential None values from Salesforce, especially for related objects
    job_id = sf_data.get('TR1__Job__r', {}).get('Id') if sf_data.get('TR1__Job__r') else None
    candidate_id = sf_data.get('TR1__Candidate__r', {}).get('Id') if sf_data.get('TR1__Candidate__r') else None
    account_id = sf_data.get('TR1__Candidate__r', {}).get('AccountId') if sf_data.get('TR1__Candidate__r') else None
    resume_txt = sf_data.get('TR1__Candidate__r', {}).get('Candidate_s_Resume_TXT__c') if sf_data.get('TR1__Candidate__r') else None
    firstname = sf_data.get('TR1__Candidate__r', {}).get('FirstName') if sf_data.get('TR1__Candidate__r') else None
    
    screening_transcript_c = sf_data.get('screening_transcript_c')
    internal_interview_transcript_c = sf_data.get('internal_interview_transcript_c')
    x1st_interview_transcript_c = sf_data.get('x1st_interview_transcript_c')
    # Use the actual field name from your SOQL query (corrected or original)
    x2nd_interview_transcript_c = sf_data.get('x2nd_interview_transcript_c') # or X2nd_Interveiw_transcript__c

    # Ensure your od_data table has a column to store the Salesforce TR1__Opportunity_Discussed__c.Id
    # Let's assume this column is named 'salesforce_opp_discussed_id'
    update_query = """
        UPDATE od_data SET
            job_id = %s,
            candidate_id = %s,
            account_id = %s,
            resume_txt = %s,
            firstname = %s,
            screening_transcript_c = %s,
            internal_interview_transcript_c = %s,
            x1st_interview_transcript_c = %s,
            x2nd_interview_transcript_c = %s,
            updated_at = CURRENT_TIMESTAMP
        WHERE od_id = %s 
    """ 
    # Add or remove fields in the SET clause to match your od_data table schema

    values_to_update = (
        job_id,
        candidate_id,
        account_id,
        resume_txt,
        firstname,
        screening_transcript_c,
        internal_interview_transcript_c,
        x1st_interview_transcript_c,
        x2nd_interview_transcript_c,
        salesforce_opp_discussed_id # For the WHERE clause
    )

    conn = None
    updated_rows = 0
    try:
        conn = get_db_connection()
        with conn.cursor() as cur:
            app.logger.info(f"Executing update query for salesforce_opp_discussed_id: {salesforce_opp_discussed_id}")
            cur.execute(update_query, values_to_update)
            updated_rows = cur.rowcount
            conn.commit()
            app.logger.info(f"Successfully updated {updated_rows} row(s) in PostgreSQL.")
    except psycopg2.Error as e:
        if conn:
            conn.rollback() # Rollback on error
        app.logger.error(f"Error updating PostgreSQL record: {e}")
        raise
    finally:
        if conn:
            conn.close()
    return updated_rows


@app.route('/sync-record', methods=['POST'])
def sync_record_endpoint():
    """
    Webhook endpoint to receive an od_id (Salesforce Opportunity Discussed Id),
    fetch data from Salesforce, and update PostgreSQL.
    Expects JSON payload: {"od_id": "salesforce_id_value"}
    """
    if not request.is_json:
        return jsonify({"error": "Request must be JSON"}), 400

    data = request.get_json()
    salesforce_opp_discussed_id = data.get('od_id')

    if not salesforce_opp_discussed_id:
        return jsonify({"error": "Missing 'od_id' in request body"}), 400

    app.logger.info(f"Received request to sync record for od_id: {salesforce_opp_discussed_id}")

    try:
        sf_record_data = fetch_salesforce_data(salesforce_opp_discussed_id)

        if not sf_record_data:
            return jsonify({"message": f"No data found in Salesforce for ID {salesforce_opp_discussed_id}"}), 404

        updated_row_count = update_postgres_record(salesforce_opp_discussed_id, sf_record_data)
        
        if updated_row_count > 0:
            return jsonify({
                "message": f"Successfully synchronized data for ID {salesforce_opp_discussed_id}",
                "salesforce_data_retrieved": True,
                "postgres_rows_updated": updated_row_count
            }), 200
        else:
            # This could mean the salesforce_opp_discussed_id was not found in your od_data table
            return jsonify({
                "message": f"Data retrieved from Salesforce for ID {salesforce_opp_discussed_id}, but no matching record found in PostgreSQL to update.",
                "salesforce_data_retrieved": True,
                "postgres_rows_updated": 0
            }), 404 # Or 200 with a different message, depending on desired behavior

    except SalesforceAuthenticationFailed:
        return jsonify({"error": "Salesforce authentication failed. Check credentials."}), 500
    except psycopg2.Error as db_err: # Catch specific DB errors if get_db_connection re-raises them
        return jsonify({"error": f"Database operation failed: {str(db_err)}"}), 500
    except Exception as e:
        app.logger.error(f"An unexpected error occurred: {e}")
        return jsonify({"error": f"An unexpected error occurred: {str(e)}"}), 500


if __name__ == '__main__':
    # For development only. For production, use a WSGI server like Gunicorn.
    # Example: gunicorn -w 4 -b 0.0.0.0:5000 app:app
    app.run(debug=True, host='0.0.0.0', port=5000)
