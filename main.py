import re
import json
import requests
import time
import pandas as pd
import psycopg2
from bs4 import BeautifulSoup
from langchain_ollama import OllamaLLM
from langchain_core.prompts import ChatPromptTemplate
from selenium import webdriver
from selenium.webdriver.chrome.options import Options

# PostgreSQL database configuration
db_config = {
    "host": "localhost",
    "database": "Web_Scrapping",
    "user": "postgres",
    "password": "Thasneem@postgre",
    "port": 5432
}

# Scraping headers
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.212 Safari/537.36'
}

# LangChain prompt template
template = (
    "Translate any non-English content to English and extract the following details "
    "from the provided text: {dom_content}. "
    "Please follow these instructions carefully:\n\n"
    "1. If no meaningful content is available or scraping fails, return the string "
    "'Not able to scrape' for all fields.\n"
    "2. Description: Extract a brief company description.\n"
    "3. Products/Services: List the products or services the company offers.\n"
    "4. Use Cases: Extract use cases where the company's offerings are applied.\n"
    "5. Customers: Identify key customers of the company.\n"
    "6. Partners: Identify the company's partners.\n"
    "Provide your output strictly as a JSON object with the keys: "
    "'description', 'products_services', 'use_cases', 'customers', 'partners'. "
    "Ensure all information is strictly in English only."
)

# Initialize the Ollama LLM model
model = OllamaLLM(model="llama2", temperature='0.7')

# Selenium WebDriver setup
chrome_options = Options()
chrome_options.add_argument("--headless")  # Headless mode
driver = webdriver.Chrome(options=chrome_options)

def get_all_links(url):
    """Extract all sub-URLs from the given page."""
    try:
        driver.get(url)
        time.sleep(2)  # Adjust this delay based on page load time
        soup = BeautifulSoup(driver.page_source, 'html.parser')

        links = set()
        for a_tag in soup.find_all('a', href=True):
            link = a_tag['href']
            if link.startswith('http') or link.startswith('/'):
                full_url = link if link.startswith('http') else url + link
                links.add(full_url)
        return links
    except Exception as e:
        print(f"Error extracting links from {url}: {e}")
        return set()

def scrape_content(url):
    """Scrape content from the given URL."""
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')
        page_content = soup.get_text(separator=' ').strip()
        return ' '.join(page_content.split())  # Clean extra whitespace
    except requests.exceptions.RequestException as e:
        print(f"Failed to retrieve {url}. Error: {e}")
        return ""

def scrape_all_content(start_url):
    """Scrape the content from all sub-URLs of a given start URL."""
    all_links = get_all_links(start_url)
    all_content = ""
    for link in all_links:
        print(f"Scraping: {link}")
        content = scrape_content(link)
        if content:
            all_content += content + "\n\n"
        time.sleep(2)  # Avoid overwhelming the server
    return all_content

def parse_with_ollama(dom_content):
    """Send content to Ollama LLM and parse the response."""
    print("Running llama2")
    prompt = ChatPromptTemplate.from_template(template)
    chain = prompt | model

    try:
        response = chain.invoke({"dom_content": dom_content})
        print("Raw response:", response)

        # Logging the raw response for debugging purposes, using utf-8 encoding
        with open("raw_responses.log", "a", encoding="utf-8") as log_file:
            log_file.write(response + "\n")

        return clean_and_parse_json(response)
    except (json.JSONDecodeError, TypeError):
        print("Failed JSON parsing. Using fallback extraction.")
        return fallback_extraction(response)

def clean_and_parse_json(response):
    """Attempt to clean the LLM response and parse it as JSON."""
    try:
        # Remove any non-JSON text from the response
        start_index = response.find('{')
        end_index = response.rfind('}')
        json_response = response[start_index:end_index + 1]

        return json.loads(json_response)
    except json.JSONDecodeError:
        print("Error parsing JSON after cleaning.")
        return fallback_extraction(response)

def fallback_extraction(text):
    """Extract fields using regex as a fallback."""
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
    row_data = {
        "id": data.get("id", ""),
        "description": data.get("description", ""),
        "products_services": data.get("products_services", ""),
        "use_cases": data.get("use_cases", ""),
        "customers": data.get("customers", ""),
        "partners": data.get("partners", "")
    }

    df = pd.DataFrame([row_data])

    try:
        existing_df = pd.read_excel(filename)
        df = pd.concat([existing_df, df], ignore_index=True)
    except FileNotFoundError:
        pass

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
                # **Join list elements into comma-separated strings**
                customers = ", ".join(data['customers'])
                partners = ", ".join(data['partners'])
                products_services = ", ".join(data['products_services'])
                use_cases = ", ".join(data['use_cases'])

                cursor.execute(query, (
                    data['description'],
                    products_services,  # Updated line
                    use_cases,          # Updated line
                    customers,          # Updated line
                    partners,           # Updated line
                    data['id']
                ))
                print(f"Updated company ID {data['id']} in the database.")
    except psycopg2.Error as e:
        print(f"Database update error: {e}")

def process_company(company_id, company_url):
    """Process a single company: scrape, extract, save, and update."""
    print(f"Processing company ID {company_id} with URL {company_url}")
    all_content = scrape_all_content(company_url)

    if all_content.strip():
        extracted_info = parse_with_ollama(all_content)
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

if __name__ == "__main__":
    process_all_companies()
    driver.quit()  # Ensure WebDriver is closed after execution
