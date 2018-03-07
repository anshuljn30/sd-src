from selenium import webdriver
import cfscrape
import time
from selenium.webdriver.common.keys import Keys


def download(dates):
    file_path = "C:\\Users\\Administrator\\Documents\\sd-src\\sql_data\\mutual_fund_data\\raw\\franklin_templeton\\"
    chrome_driver = "C:\\Users\\Administrator\\Downloads\\chromedriver_win32\\chromedriver.exe"
    scraper = cfscrape.create_scraper()
    driver = webdriver.Chrome(executable_path=chrome_driver)
    driver.get("https://www.franklintempletonindia.com/investor/reports")

    webdriver.ActionChains(driver).send_keys(Keys.ESCAPE).perform()
    time.sleep(2)
    dropdwn_panel = driver.find_element_by_xpath('.//span[contains(text(), "Disclosure of AUM by Geography")]')
    dropdwn_panel.click()
    time.sleep(2)
    dropdwn_panel = dropdwn_panel.find_element_by_xpath('../following-sibling::ul')
    dropdwn_panel = dropdwn_panel.find_element_by_xpath('.//li[@id = "MonthlyPortfolioDisclosure"]')
    dropdwn_panel.click()
    time.sleep(2)

    for d in dates:
        month = d.strftime('%b')
        year = d.strftime('%Y')

        file = driver.find_element_by_xpath('.//span[contains(text(), "ISIN Report") and \
                                contains(text(),"' + month + '") and contains(text(),"' + year + '")]')
        file = file.find_element_by_xpath('..')
        file_link = file.get_attribute('href')

        cfurl = scraper.get(file_link)
        save_file_name = "franklin_templeton_portfolios_" + d.strftime('%Y%m') + '.xlsx'

        print('Downloading file for ' + d.strftime('%b%Y'))
        with open(file_path + save_file_name, 'wb') as f:
            f.write(cfurl.content)

    driver.close()