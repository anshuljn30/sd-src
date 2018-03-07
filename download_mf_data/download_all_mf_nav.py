from selenium import webdriver
import cfscrape
import time


def download(dates):
    file_path = "C:\\Users\\Administrator\\Documents\\sd-src\\sql_data\\mutual_fund_data\\raw_nav\\"
    url = "http://portal.amfiindia.com/DownloadNAVHistoryReport_Po.aspx?tp=1&"
    scraper = cfscrape.create_scraper()

    for d in dates:
        date = d.strftime('%d-%b-%Y')
        file_link = url + 'frmdt=' + date + '&todt=' + date

        cfurl = scraper.get(file_link)
        save_file_name = "all_mf_nav_" + date + '.xlsx'

        print('Downloading file for ' + date)
        with open(file_path + save_file_name, 'wb') as f:
            f.write(cfurl.content)
