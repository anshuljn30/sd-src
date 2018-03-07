from selenium import webdriver
import cfscrape
import time


def download(dates):
    file_path = "C:\\Users\\Administrator\\Documents\\sd-src\\sql_data\\mutual_fund_data\\raw\\motilal_oswal\\"
    chrome_driver = "C:\\Users\\Administrator\\Downloads\\chromedriver_win32\\chromedriver.exe"
    scraper = cfscrape.create_scraper()
    driver = webdriver.Chrome(executable_path=chrome_driver)
    driver.get("https://www.motilaloswalmf.com/downloads/mutual-fund/Month-End-Portfolio")

    for d in dates:
        month = d.strftime('%b')
        year = d.strftime('%Y')

        file = []
        while not file:
            try:
                file = driver.find_element_by_xpath('//td[contains(text(), "Month End Portfolio") and \
                                            contains(text(),"' + month + '") and contains(text(),"' + year + '")]')
            except:
                driver.find_element_by_xpath('.//a[text()="Next"]').click()
                time.sleep(2)

        file = file.find_element_by_xpath('following-sibling::td/following-sibling::td//a')
        file_link = file.get_attribute('href')

        cfurl = scraper.get(file_link)
        save_file_name = "motilal_oswal_portfolios_" + d.strftime('%Y%m') + '.xlsx'

        print('Downloading file for ' + d.strftime('%b%Y'))
        with open(file_path + save_file_name, 'wb') as f:
            f.write(cfurl.content)

        driver.refresh()
        time.sleep(2)
