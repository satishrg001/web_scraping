# Author : Satish Gaikwad
# Purpose: This program is used to scrape the data from a webpage and to save the scraped data into Excel sheet.
#          It also saves the webpages and it's sub-pages into .html files and zip it.
# Version: 1.0
# Date: 31-JUL-2024
# Requirement: https://github.com/satishrg001/web_scraping/etl_task.docx


# Import required Python modules
import Parameters
import warnings
import httplib2
from bs4 import BeautifulSoup, SoupStrainer
import re
from pywebcopy import save_webpage
import logging
import requests
from socket import timeout
from urllib.error import HTTPError, URLError
from datetime import datetime
import shutil
import pandas as pd


start_time = datetime.now()
print("Scraping Process Started at", start_time)

# Setting the warnings to be ignored
warning_switch_p = Parameters.warning_switch
warnings.filterwarnings(warning_switch_p)

# Configure logging
current_date_time = datetime.now().strftime('%H_%M_%S_%d_%m_%Y')
logfile_path_p = Parameters.logfile_path
logging.basicConfig(filename=logfile_path_p + "scraping_" + current_date_time + ".log", filemode="a",
                    format="%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s", datefmt="%H:%M:%S",
                    level=logging.DEBUG)

logging.info("Scraping Process Started....")

# Define variables
prj_folder_p = Parameters.prj_folder
prj_name_p = Parameters.prj_name
html_site_folder_p = Parameters.html_site_folder
p_num_prefix_p = Parameters.p_num_prefix
url_p = Parameters.url
root_dir_p = Parameters.root_dir
archive_path_p = Parameters.archive_path + current_date_time
excel_file_name_p = Parameters.excel_file_name + current_date_time + Parameters.excel_file_extension
excel_file_name_wl_p = Parameters.excel_file_name_wl + current_date_time + Parameters.excel_file_extension
sheet_name1_p = Parameters.sheet_name1
sheet_name2_p = Parameters.sheet_name2
file_extension_p = Parameters.file_extension
year_1_p = Parameters.year_1
year_2_p = Parameters.year_2
column_name_cmp_p = Parameters.column_name_cmp
group_by_column_1_p = Parameters.group_by_column_1
group_by_column_2_p = Parameters.group_by_column_2
sum_column_1_p = Parameters.sum_column_1
sum_column_2_p = Parameters.sum_column_2
html_path = ""
p_num = ""
final_url = ""
df = pd.DataFrame()

# Check url is accessible and get response
try:
    status, response = httplib2.Http().request(url_p)

except HTTPError as error:
    logging.error('HTTP Error: %s\nurl: %s', error, url_p)
except URLError as error:
    if isinstance(error.reason, timeout):
        logging.error('Timeout Error: %s\nurl: %s', error, url_p)
    else:
        logging.error('url Error:  %s\nurl: %s', error, url_p)
except Exception as error:
    logging.error('Unexpected Error:  %s\nurl: %s', error, url_p)
else:
    logging.info('url Access successful.')


# Save all the webpages including sub-pages for the url
try:
    for link in BeautifulSoup(response, 'html.parser', parse_only=SoupStrainer('a')):
        if link.has_attr('href'):
            text = link.get('href')
            # Get html content having page_num text
            if re.search("page_num", text):
                # Get the starting position of ?page_num
                page_num = text.rfind("?page_num")

                # Extract page_number
                page_num_suffix = text[page_num:]

                # Extract page_number
                p_num_position = text.rfind("=")
                p_num = text[p_num_position + 1:]

                # Prepare final url with page number
                final_url = url_p + page_num_suffix

                # Save Webpage to html file
                save_webpage(
                    url=final_url,
                    project_folder=prj_folder_p,
                    project_name=prj_name_p,
                    bypass_robots=True,
                    debug=False,
                    open_in_browser=False,
                    delay=None,
                    threaded=True,
                )
                logging.info(final_url)

                # Load the downloaded HTML file to extract data table from html webpage
                html_path = prj_folder_p + prj_name_p + html_site_folder_p + p_num_prefix_p + p_num + file_extension_p
                with open(html_path, 'r', encoding='utf-8') as file:
                    soup = BeautifulSoup(file, 'html.parser')

                # Find the table in HTML page data
                table = soup.find('table')

                # Read the HTML table into a Pandas DataFrame
                df = pd.concat([df, pd.read_html(str(table))[0]], ignore_index=True)

except requests.exceptions.ConnectionError as e:
    logging.info("Connection timeout. url->%s, Error->%s", final_url, e)
    pass
except Exception as err:
    logging.error(err)

# Sort data based on Year column in Ascending order
df = df.sort_values('Year')

# Filter the data
df = df[(df[column_name_cmp_p] >= year_1_p) & (df[column_name_cmp_p] <= year_2_p)]

# Save filtered data into Excel file for NHL Stats 1990-2011
df.to_excel(excel_file_name_p, index=False, sheet_name=sheet_name1_p)

df2 = df.groupby([group_by_column_1_p, group_by_column_2_p])[sum_column_1_p].sum()
df3 = df.groupby([group_by_column_1_p, group_by_column_1_p])[sum_column_2_p].sum()

df_concat = pd.concat([df2, df3], axis=1)

# Save wins_losses data into excel file
df_concat.to_excel(excel_file_name_wl_p, sheet_name=sheet_name2_p)

logging.info("Zipping html webpages into a zip file")

# Zip downloaded html webpages
shutil.make_archive(archive_path_p, format='zip', root_dir=root_dir_p)
logging.info("Zipped...")

logging.info("Scraping Process Completed.")

end_time = datetime.now()
print("Scraping Process Completed at", end_time)
print('Duration: {}'.format(end_time - start_time))
