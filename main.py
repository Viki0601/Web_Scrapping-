import re
import json
import requests
from bs4 import BeautifulSoup
from langchain_ollama import OllamaLLM
from langchain_core.prompts import ChatPromptTemplate
import psycopg2
import pandas as pd

# PostgreSQL database configuration
db_config = {
    "host": "localhost",
    "database": "company_details",
    "user": "postgres",
    "password": "Vignesh@0601",
    "port": 5432
}

# Scraping headers
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.212 Safari/537.36'
}

# LangChain prompt template
template = (
    "Extract the following details from the provided text: {dom_content}. "
    "Please follow these instructions carefully: \n\n"
    "1. *Description*: Extract a brief company description. \n"
    "2. *Products/Services*: List the products or services the company offers. \n"
    "3. *Use Cases*: Extract use cases where the company's offerings are applied. \n"
    "4. *Customers*: Identify key customers of the company. \n"
    "5. *Partners*: Identify the company's partners.\n"
    "Provide your output strictly as a JSON object with the keys: "
    "'description', 'products_services', 'use_cases', 'customers', 'partners'."
)

# Initialize the Ollama LLM model
model = OllamaLLM(model="llama2")

def clean_and_parse_json(response):
    """Attempt to clean the LLM response and parse it as JSON."""
    try:
        # Remove any non-JSON text from the response
        start_index = response.find('{')
        end_index = response.rfind('}')
        json_response = response[start_index:end_index + 1]

        # Attempt to parse the cleaned JSON
        return json.loads(json_response)
    except json.JSONDecodeError:
        print("Error parsing JSON after cleaning.")
        return fallback_extraction(response)

def scrape_website(url):
    """Scrape content from the provided URL."""
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'html.parser')
        page_content = soup.get_text(separator=' ').strip()
        return ' '.join(page_content.split())  # Clean extra whitespace
    except requests.exceptions.RequestException as e:
        print(f"Failed to retrieve the website. Error: {e}")
        return ""

def parse_with_ollama(dom_content):
    """Send content to Ollama LLM and parse the response."""
    prompt = ChatPromptTemplate.from_template(template)
    chain = prompt | model

    try:
        response = chain.invoke({"dom_content": dom_content})
        print("Raw response:", response)

        # Logging the raw response for debugging purposes, using utf-8 encoding
        with open("raw_responses.log", "a", encoding="utf-8") as log_file:
            log_file.write(response + "\n")

        # Attempt to clean and parse JSON response
        return clean_and_parse_json(response)
    except (json.JSONDecodeError, TypeError):
        print("Failed JSON parsing. Using fallback extraction.")
        return fallback_extraction(response)


def fallback_extraction(text):
    """Extract fields using regex as a fallback with improved patterns."""
    patterns = {
        "description": r"(?i)(?:Description\s*:\s*)(.*?)(?:\n|Products/Services|$)",
        "products_services": r"(?i)(?:Products/Services\s*:\s*)(.*?)(?:\n|Use Cases|$)",
        "use_cases": r"(?i)(?:Use Cases\s*:\s*)(.*?)(?:\n|Customers|$)",
        "customers": r"(?i)(?:Customers\s*:\s*)(.*?)(?:\n|Partners|$)",
        "partners": r"(?i)(?:Partners\s*:\s*)(.*?)(?:\n|$)"
    }

    extracted_info = {}
    for key, pattern in patterns.items():
        match = re.search(pattern, text, re.DOTALL)
        extracted_info[key] = match.group(1).strip() if match else ""

    return extracted_info

def fetch_urls_from_db():
    """Fetch all company URLs and IDs from the database."""
    try:
        conn = psycopg2.connect(**db_config)
        with conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT id, url FROM company_info")
                return cursor.fetchall()
    except psycopg2.Error as e:
        print(f"Error fetching URLs: {e}")
        return []

def save_to_excel(data, filename="extracted_data.xlsx"):
    """Save data to Excel with all fields properly populated."""
    # Ensure all columns are covered, even if some data is missing.
    row_data = {
        "id": data.get("id", ""),
        "description": data.get("description", ""),
        "products_services": data.get("products_services", ""),
        "use_cases": data.get("use_cases", ""),
        "customers": data.get("customers", ""),
        "partners": data.get("partners", "")
    }

    # Convert the row into a DataFrame
    df = pd.DataFrame([row_data])

    try:
        # If the Excel file exists, load it and append the new row.
        existing_df = pd.read_excel(filename)
        df = pd.concat([existing_df, df], ignore_index=True)
    except FileNotFoundError:
        pass  # If file doesn't exist, this will create a new one.

    # Save the DataFrame to Excel.
    df.to_excel(filename, index=False)
    print(f"Data saved to {filename}")

def update_db(data):
    """Update the database with extracted data."""
    try:
        conn = psycopg2.connect(**db_config)
        with conn:
            with conn.cursor() as cursor:
                query = """
                    UPDATE company_info
                    SET description = %s, 
                        products_services = %s, 
                        use_cases = %s,
                        customers = %s, 
                        partners = %s
                    WHERE id = %s
                """
                cursor.execute(query, (
                    data['description'],
                    data['products_services'],
                    data['use_cases'],
                    data['customers'],
                    data['partners'],
                    data['id']
                ))
                print(f"Updated company ID {data['id']} in the database.")
    except psycopg2.Error as e:
        print(f"Database update error: {e}")

def process_company(company_id, company_url):
    """Process a single company: scrape, extract, save, and update."""
    print(f"Processing company ID {company_id} with URL {company_url}")
    content = scrape_website(company_url)

    if content:
        extracted_info = parse_with_ollama(content)
        extracted_info['id'] = company_id

        save_to_excel(extracted_info)
        update_db(extracted_info)
    else:
        print(f"No content available for company ID {company_id}.")

def process_all_companies():
    """Fetch companies and process them one by one."""
    companies = fetch_urls_from_db()
    if not companies:
        print("No companies found in the database.")
        return

    for company_id, company_url in companies:
        process_company(company_id, company_url)
# Function to remove curly braces from the string fields
def remove_curly_braces(text):
    return re.sub(r'[{}]', '', text) if isinstance(text, str) else text


if __name__ == "__main__":
    process_all_companies()
