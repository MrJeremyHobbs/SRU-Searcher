import csv
import sys
from almar import SruClient
import concurrent.futures
import itertools
import os
import re

# inputs from user
campus_code = 'campus_code'
input_file = ".\\data\\book_circles.csv"
search_index = "isbn".upper()
delimiter = "; "

# initialize SRU clients
iz = SruClient('https://iz_url')
nz = SruClient('https://nz_url')

# turn input file into list
with open(input_file, 'r', encoding="utf-8") as f:
    reader = csv.reader(f)
    rows = list(reader)

def main():
    # pop header, add SRU columns, and add back to rows
    header = rows.pop(0)
    header.extend(('PRINT HOLDINGS (SRU)', 'ELECRONIC HOLDINGS (SRU)'))
    rows.insert(0, header)
    
    # find search index column
    search_index_column = header.index(search_index)
    
    # parse rows (skip header)
    for row in rows[1:]:
        
        # split search index by delimiter
        search_indexes = row[search_index_column].split(delimiter)
    
        # generate SRU queries
        sru_queries = []
        for index in search_indexes:
            if index:
                sru_query = f'alma.{search_index.lower()}="{index}"'
                sru_queries.append(sru_query)
                
        # perform SRU queries in batch
        if sru_queries:
            records = batch_sru(sru_queries)
    
            # get holdings
            print_holdings = get_print_holdings(records)
            electronic_holdings = get_electronic_holdings(records)
            
            # generate holdings statements
            print_holdings_statement = generate_print_holdings_statement(print_holdings)
            electronic_holdings_statement = generate_electronic_holdings_statement(electronic_holdings)
            
            # add results to row
            row.extend((print_holdings_statement, electronic_holdings_statement))
            
        # console output
        print(print_holdings_statement)
        print(electronic_holdings_statement)
        print("------------------------------------")
        sys.stdout.flush()
        
    # write to output and open
    with open("output.csv", "w", encoding="utf-8", newline='') as f:
        writer = csv.writer(f)
        writer.writerows(rows)

    os.system('output.csv')

    
# holdings functions ##########################################################
def get_print_holdings(records):
    print_holdings = []
    
    for record in records:
        for field in record.get_fields():
            print_holding = {}
            if field.tag == "AVA": # this tag only exists in IZ output
                for subfield in field.node:
                    print_holding[subfield.attrib['code']] = subfield.text
                    print_holdings.append(print_holding)
                    
    return print_holdings
    
def get_electronic_holdings(records):
    electronic_holdings = []
    
    for record in records:
        for field in record.get_fields():
            electronic_holding = {}
            if field.tag == "AVE":
                for subfield in field.node:
                    electronic_holding[subfield.attrib['code']] = subfield.text
                    electronic_holdings.append(electronic_holding)
                    
    return electronic_holdings

def generate_print_holdings_statement(print_holdings):
    print_holdings_statements = []
    for holding in print_holdings:
        location = holding.get('c', '')
        call_number = holding.get('d', '')
        range = holding.get('t', '')
        holding_statement = f"{location} ({call_number}) {range}"
        print_holdings_statements.append(holding_statement)
        
    # remove duplicate holdings
    print_holdings_statements = list(set(print_holdings_statements))
    print_holdings_statement = "\n".join(print_holdings_statements)
    
    return print_holdings_statement

def generate_electronic_holdings_statement(electronic_holdings):
    electronic_holdings_statements = []
    for holding in electronic_holdings:
        platform = holding.get('m', '')
        range = holding.get('s', None)
        
        if range:
            holding_statement = f"{platform} ({range})"
        else:
            holding_statement = f"{platform}"
        
        electronic_holdings_statements.append(holding_statement)
    
    # remove duplicate holdings
    electronic_holdings_statements = list(set(electronic_holdings_statements))
    
    # sort by availability column
    r = re.compile(r'(\(Available from .*\))')
    try:
        sorted_electronic_holdings = sorted(electronic_holdings_statements, key=lambda x:r.search(x).group(1))
    except AttributeError:
        sorted_electronic_holdings = electronic_holdings_statements
    
    # join final list
    electronic_holdings_statement = "\n".join(sorted_electronic_holdings)
    
    return electronic_holdings_statement
    
# async functions #############################################################                           
def load_query(query):
    records = []
    # search IZ
    for record in iz.search(query):
        records.append(record)
    # search NZ
    for record in nz.search(query):
        records.append(record)
    
    return records
    
def batch_sru(queries="", workers=""):
    workers = len(queries)
    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
        sru_responses = executor.map(load_query, queries)
    
    # flatten sru_responses list
    records = list(itertools.chain.from_iterable(sru_responses))   
    
    return records
                

# top level            
if __name__ == "__main__":
    main()