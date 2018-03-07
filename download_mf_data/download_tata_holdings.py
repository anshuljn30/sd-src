from selenium import webdriver
import cfscrape
import time


def download(dates):
    file_path = "C:\\Users\\Administrator\\Documents\\sd-src\\sql_data\\mutual_fund_data\\raw\\tata\\"
    chrome_driver = "C:\\Users\\Administrator\\Downloads\\chromedriver_win32\\chromedriver.exe"
    scraper = cfscrape.create_scraper()
    driver = webdriver.Chrome(executable_path=chrome_driver)
    driver.get("http://www.tatamutualfund.com/downloads/monthly-portfolio")

    portfolio_panel = driver.find_element_by_xpath('.//div[@class="dataH"]')

    for d in dates:
        month = d.strftime('%b')
        year = d.strftime('%Y')

        year_panel = portfolio_panel.find_element_by_xpath('.//a[contains(text(), "' + year + '")]')
        year_panel.click()
        year_panel = year_panel.find_element_by_xpath('ancestor::h4/following-sibling::div')

        file = year_panel.find_element_by_xpath('.//a[contains(text(), "Portfolio") and \
                                contains(text(),"' + month + '") and contains(text(),"' + year + '")]')
        file_link = file.get_attribute('href')

        cfurl = scraper.get(file_link)
        save_file_name = "tata_portfolios_" + d.strftime('%Y%m') + '.xlsx'

        print('Downloading file for ' + d.strftime('%b%Y'))
        with open(file_path + save_file_name, 'wb') as f:
            f.write(cfurl.content)

    driver.close()