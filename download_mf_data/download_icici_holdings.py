from selenium import webdriver
import cfscrape
import time
import io
import zipfile
import xlwings as xw
import os


def download(dates):
    file_path = "C:\\Users\\Administrator\\Documents\\sd-src\\sql_data\\mutual_fund_data\\raw\\icici_prudential\\"
    chrome_driver = "C:\\Users\\Administrator\\Downloads\\chromedriver_win32\\chromedriver.exe"
    scraper = cfscrape.create_scraper()
    driver = webdriver.Chrome(executable_path=chrome_driver)
    driver.get("http://www.icicipruamc.com/Downloads/MonthlyPortfolioDisclosure.aspx")

    for d in dates:
        month = d.strftime('%b')
        year = d.strftime('%y')

        file = driver.find_element_by_xpath('.//a[contains(text(), "Monthly Portfolio Disclosure") and \
                                contains(text(),"' + month + '") and contains(text(),"' + year + '")]')
        file_link = file.get_attribute('href')
        save_file_name = "icici_prudential_portfolios_" + d.strftime('%Y%m') + '.xlsx'

        cfurl = scraper.get(file_link)
        contents = io.BytesIO(cfurl.content)
        if zipfile.is_zipfile(contents):
            zip = zipfile.ZipFile(contents)
            xl_names = zip.namelist()

            print('Downloading file for ' + d.strftime('%b%Y'))
            for xl_name in xl_names:
                xl = zip.extract(xl_name, file_path)
                wb = xw.Book(xl)
                new_wb = xw.Book(file_path + save_file_name)

                for sheet in wb.sheets:
                    new_wb.sheets[sheet.name] = sheet

        os.remove(os.path.dirname(xl))



    driver.close()