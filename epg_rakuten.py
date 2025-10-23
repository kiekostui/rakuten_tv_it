import requests
from datetime import datetime, timedelta, UTC
import time
import json
import xml.etree.ElementTree as ET
import sys


# --- Configuration Constants ---

EPG_TIMEFRAME = 48 # Time window in hours for EPG
CHANNELS_CHUNK = 25 # Number of channels per single request
TIMEFRAME_CHUNK = 4 # Time window chunk for single request
MARKET = 'it'
CLASSIFICATION_ID = '36'
OUTPUT_FILE = f'epg_rakuten_it.xml'


# --- Time Conversion Function ---

def date_converter(date_string):
    """
    Converts the ISO 8601 string to the XMLTV format (YYYYMMDDhhmmss ±zzzz).
    Retains the time zone offset from the source data.
    """
    try:    
        # fromisoformat correctly handles the ISO string with timezone (e.g., +02:00)
        date_obj = datetime.fromisoformat(date_string)
        if date_obj.tzinfo:
            # Format with the source offset (e.g., +0200)
            date_formatted = date_obj.strftime('%Y%m%d%H%M%S %z')
        else:
            # If no timezone is present, use UTC
            date_formatted = date_obj.strftime('%Y%m%d%H%M%S +0000')
        return date_formatted
    except:
        # Returns an empty string in case of conversion error
        return ''


# --- JSON Parsing and XML Writing Function ---

def json_parse(epg_dict, epg_xml):
    """
    Writes all <channel> tags first, and then all <programme> tags 
    in the XMLTV format, ensuring the correct order through double iteration.
    """
    
    # FIRST ITERATION: Add all channels
    for channel_id, value in epg_dict.items():

        channel = ET.SubElement(epg_xml, 'channel')
        channel.attrib['id'] = channel_id
        
        name = ET.SubElement(channel, 'display-name')
        name.text = value['display-name'] 
        
        lcn = ET.SubElement(channel, 'lcn')
        # LCN is guaranteed to be a string by append_info
        lcn.text = value['lcn'] 
            
        icon = ET.SubElement(channel, 'icon')
        icon.attrib['src'] = value['icon']

    # SECOND ITERATION: Add all programs
    for channel_id, value in epg_dict.items(): 
        programs = value['programs']
        
        for start, program_info in programs.items(): 
            programme = ET.SubElement(epg_xml, 'programme')
            
            # Main program attributes with time conversion
            programme.attrib['start'] = date_converter(start)
            programme.attrib['stop'] = date_converter(program_info['stop'])
            programme.attrib['channel'] = channel_id
            
            # Internal program elements (robust values against 'null')
            title = ET.SubElement(programme, 'title')
            title.text = program_info['title']
            
            desc = ET.SubElement(programme, 'desc')
            desc.text = program_info['desc']
            
            icon = ET.SubElement(programme, 'icon')
            icon.attrib['src'] = program_info['icon']


# --- Aggregation Function ---

def append_info(epg_dict, chunk_info_list):
    """
    Aggregates data from the JSON chunk. Forcing to str(value or '') handles 
    'null'/'None' values, making the script robust.
    """

    for element in chunk_info_list:
        ch_id = element.get('id', '')
        if not ch_id:
            continue
            
        # 1. De-duplication and Channel Initialization
        if ch_id not in epg_dict:
            epg_dict[ch_id] = {
                # Use 'or' to catch None/null and fallback
                'display-name': str(element.get('title', 'No name') or 'No name'), 
                'lcn': str(element.get('channel_number', '') or ''),
                'icon': str(element.get('images', {}).get('artwork', '') or ''),
                'programs': {} 
                }
            
        # 2. Program Aggregation
        source_program_list = element.get('live_programs', [])

        for source_program in source_program_list:
            program_key = str(source_program.get('starts_at','') or '')
            if not program_key:
                continue
            
            epg_dict[ch_id]['programs'][program_key] = {
                'stop':str(source_program.get('ends_at', '') or ''),
                'channel':ch_id,
                'title':str(source_program.get('title', '') or ''),
                'desc':str(source_program.get('description', '') or ''),
                'icon':str(source_program.get('images', {}).get('snapshot','') or '')
                }
                
                
# --- JSON Request Function ---

def get_json(url, start_epg_iso, end_epg_iso, page, MARKET, CLASSIFICATION_ID):
    """
    Executes the API request and handles HTTP and JSON errors.
    """

    params = {
        'CLASSIFICATION_ID':CLASSIFICATION_ID,
        'device_identifier':'web',
        'epg_ends_at':f'{end_epg_iso}',
        'epg_starts_at':f'{start_epg_iso}',
        'locale':f'it',
        'market_code':f'it',
        'page':f'{str(page)}',
        'per_page':f'{str(CHANNELS_CHUNK)}'
        }

    time.sleep(1) 

    try:
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        
    except requests.exceptions.RequestException as e:
        print (f'Error during request of {url} with params:\n'
               ''.join(f'{k}={v}\n' for k, v in params.items())
               )
        return None

    try:
        file_json = response.json()
    except:
        print(f'No valid json for {url} with params:\n'
              ''.join(f'{k}={v}\n' for k, v in params.items())
              )
        return {'data':'error'}

    return file_json


# --- MAIN LOGIC ---

if __name__ == '__main__':
    start = datetime.now(UTC)
    print(f'start: {start.strftime('%Y-%m-%d %H:%M:%S')}')

    # Truncate time: hh:00:00,000
    start_time = start.replace(minute=0, second=0, microsecond=0)
    print(f'start_time: {start_time.strftime('%Y-%m-%d %H:%M:%S')}')

    url = 'https://gizmo.rakuten.tv/v3/live_channels'

    epg_dict = {}

    # Initialize page number
    page = 1

    while True:
        print(f'Processing page {page}')
        # Initialize time chunk cycle counter
        n = 1
        # Initialize time start for epg:
        start_epg = start_time
        json_chunk = None # Variable for loop exit control

        while (n * TIMEFRAME_CHUNK) <= EPG_TIMEFRAME:
            end_epg = start_epg + timedelta(hours = TIMEFRAME_CHUNK)
            
            # Format ISO dates for the API (terminate with .000Z for UTC)
            start_epg_iso = start_epg.strftime('%Y-%m-%dT%H:%M:%S.000Z')
            end_epg_iso = end_epg.strftime('%Y-%m-%dT%H:%M:%S.000Z')
            
            print(f'start: {start_epg_iso}, end: {end_epg_iso}')
            
            json_chunk = get_json(url, start_epg_iso, end_epg_iso, page, MARKET, CLASSIFICATION_ID)
            start_epg = end_epg # Moves start_epg to the end of the previous chunk

            if json_chunk.get('data', '') == 'error':
                    sys.exit()

            if json_chunk:
                try:
                    chunk_info_list = json_chunk.get('data', [])
                except:
                    # Break the time window loop if the response is not valid JSON
                    break 
                
                if not chunk_info_list: 
                    print (f'No data for page {page}, from {start_epg_iso} to {end_epg_iso}')
                    if n == 1:
                       json_chunk = None # For external loop exit control
                    break
                
                append_info(epg_dict, chunk_info_list)
                n += 1 # Increment only if data is valid

            else:
                break # Break the time window loop in case of request error

        # Break if the result is empty in the first time window
        if (n == 1) and (not json_chunk):  
            break
        
        # Go to the next page
        page += 1

    # Root element for epg in xml format
    epg_xml = ET.Element('tv')
    # Add attributes required by the XMLTV standard
    epg_xml.attrib['source-info-url'] = 'none'
    epg_xml.attrib['source-info-name'] = 'none'

    json_parse(epg_dict, epg_xml)

    # Create the XML tree and format the output with 4-space indentation
    tree = ET.ElementTree(epg_xml)
    ET.indent(tree, space='    ', level=0) 

    print('Writing output epg file')

    # Write the XML file
    tree.write(OUTPUT_FILE, encoding='utf-8', xml_declaration=True)

    end = datetime.now(UTC)

    print(f'End: {end.strftime('%Y-%m-%d %H:%M:%S')}')
