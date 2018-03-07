from selenium import webdriver
import cfscrape
import time
import re


def download(dates):
    file_path = "C:\\Users\\Administrator\\Documents\\sd-src\\sql_data\\mutual_fund_data\\raw\\boi_axa\\"
    chrome_driver = "C:\\Users\\Administrator\\Downloads\\chromedriver_win32\\chromedriver.exe"
    scraper = cfscrape.create_scraper()
    driver = webdriver.Chrome(executable_path=chrome_driver)
    driver.get("http://www.boiaxa-im.com/downloads/download.php")

    driver.find_element_by_xpath('.//select[@id="down_cat_desc"]/option[text()="All"]').click()
    time.sleep(2)
    driver.find_element_by_xpath('.//select[@id="download"]/option[text()="Monthly Portfolio"]').click()
    time.sleep(2)

    for d in dates:
        month = d.strftime('%b')
        year = d.strftime('%Y')

        file = driver.find_element_by_xpath('.//td[contains(text(),"' + month + '") and contains(text(),"' + year + '")]')
        file = file.find_element_by_xpath('following-sibling::td//a')
        file_link = file.get_attribute('onclick')
        file_link = re.findall(r"'([^']*)'", file_link)
        file_link = 'http://www.boiaxa-im.com' + file_link[0]

        cfurl = scraper.get(file_link)
        save_file_name = "boi_axa_portfolios_" + d.strftime('%Y%m') + '.xlsx'

        print('Downloading file for ' + d.strftime('%b%Y'))
        with open(file_path + save_file_name, 'wb') as f:
            f.write(cfurl.content)
