import requests
from bs4 import BeautifulSoup
import json
import sys
import os
from copy import deepcopy

# Base URL to MSFT Docs
base_url = "https://learn.microsoft.com/"
# Specific path for tables supported with the Log Ingestion API
supportedTablesPath = "/en-us/azure/azure-monitor/logs/logs-ingestion-api-overview#supported-tables"
# Used to filter links returned when scraping the MSFT page
tablesPath = "/en-us/azure/azure-monitor/reference/tables/"

# Return list of Azure Monitor Tables that support API Ingestion
def get_supported_tables(url, filter):
    # Scrape the Tables that support API Ingestion
    response = requests.get(url)
   
    if response.status_code == 200:
        # Parse the HTML, and look for all link tags with href
        soup = BeautifulSoup(response.content, 'html.parser')
        links = soup.find_all('a', href=True)
    
        # Create empty table array 
        supportedTables = []

        # For each link tag found, grab the href value,
        # Ensure it is a link to a table using the regex 
        # Append the table name to the table array
        for link in links:
            href = link.get('href')

            if href and href.startswith(filter):
                supportedTables.append(link.text)

        return supportedTables
    
    else:
        return None

# Return required fields and data types for requested Azure Monitor Table
def parseTableDetails(base_url,path,table):
    # Scrape HTML from the specific Table schema documentation
    response = requests.get(base_url + path + table)

    if response.status_code == 200:
        table_dict = []
        # Parse the HTML request, grab the last table on the page 
        # This should be the required fields and types
        # Then, grab the rows within the table
        rows = BeautifulSoup(response.content, 'html.parser').find_all('table')[-1].find_all('tr')

        # Iterate through all the rows skipping the first row (header)
        # Skip any field names that start with '_' (internal Sentinel fields)
        # Grab the first two td elements within the row, create a dict, and append that dict to table_dict array
        for row in rows[1:]:
            if not(row.text.strip().startswith('_') or row.text.strip().startswith('TenantId')):
                cells = row.find_all('td')
                dict_entry = {'name': cells[0].text, 'type': cells[1].text, 'description': cells[2].text}
                table_dict.append(dict_entry)
            
        return table_dict
    
    else:
        return None

# Function to generate JSON schema format
def generateJSONSchema(tableDetails, tableName):
    jsonSchema = {}
    jsonSchema["$id"] = "https://example.com/schema/" + tableName + ".json"
    jsonSchema["$schema"] = "http://json-schema.org/draft-07/schema#"
    jsonSchema["title"] = tableName
    jsonSchema["description"] = "JSON Schema for Log Analytics table: " + tableName
    jsonSchema["type"] = "object"
    jsonSchema["required"] = ["TimeGenerated", "Type"]
    jsonSchema["$defs"] = {"time": {"anyOf": [{"type": "string", "format": "date-time"}, {"type": "string", "pattern": "\\d{10}\\.?\\d{0,3}?"}]}}
    jsonSchema["properties"] = {}
    

    for item in tableDetails:

        if item['type'] == 'datetime':
            entry = {'$ref': '#/$defs/time', 'description': item['description']}
        elif item['type'] == 'int' or item['type'] == 'long':
            entry = {'type': 'integer', 'description': item['description']}
        elif item['type'] == 'bool':
            entry = {'type': 'boolean', 'description': item['description']}
        elif item['type'] == 'dynamic':
            entry = {'type': 'string', 'description': item['description']}
        elif item['type'] == 'real':
            entry = {'type': 'number', 'description': item['description']}
        elif item['name'] == 'Type':
            entry = {'type': item['type'], 'description': item['description'], 'enum': [tableName]}
        else:
            entry = {'type': item['type'], 'description': item['description']}

        jsonSchema['properties'][item['name']] = entry

    return(jsonSchema)

# Write DCR Deployment Templates to file
def writeSchema(schema):
   
    path = 'jsonSchema/'
    os.makedirs(path, exist_ok=True)

    filename = schema['title'] + '-schema.json'
    schema = json.dumps(schema)

    with open(path + filename, 'w') as f:
        f.write(schema)

if __name__ == '__main__':

    supportedTables = get_supported_tables(base_url + supportedTablesPath, tablesPath)

    #schema = generateJSONSchema(parseTableDetails(base_url,tablesPath,"Syslog"), "Syslog")

    for table in supportedTables:
        schema = generateJSONSchema(parseTableDetails(base_url,tablesPath,table), table)
        writeSchema(schema)
