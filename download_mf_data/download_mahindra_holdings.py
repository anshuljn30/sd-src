from selenium import webdriver
import cfscrape
import time


def download(dates):
    file_path = "C:\\Users\\Administrator\\Documents\\sd-src\\sql_data\\mutual_fund_data\\raw\\mahindra\\"
    chrome_driver = "C:\\Users\\Administrator\\Downloads\\chromedriver_win32\\chromedriver.exe"
    scraper = cfscrape.create_scraper()
    options = webdriver.ChromeOptions()
    options.add_argument("--start-maximized")
    driver = webdriver.Chrome(executable_path=chrome_driver, chrome_options=options)
    driver.get("http://www.mahindramutualfund.com/downloads#MANDATORY-DISCLOSURES")
    time.sleep(3)
    disclaimer_panel = driver.find_element_by_xpath('.//a[text()="I AM NOT A US PERSON/RESIDENT OF CANADA"]')
    disclaimer_panel.click()
    time.sleep(3)

    for d in dates:
        month = d.strftime('%B')
        year = d.strftime('%Y')

        portfolio_panel = driver.find_element_by_xpath('.//a[text()="Monthly Portfolio Disclosure"]')
        portfolio_panel.click()
        portfolio_panel = portfolio_panel.find_element_by_xpath('ancestor::h2/following-sibling::div')

        time.sleep(1)
        year_panel = portfolio_panel.find_element_by_link_text(year)
        year_panel.click()

        time.sleep(1)
        file = driver.find_element_by_xpath('//a[contains(text(), "Monthly Portfolio Disclosure") and contains(text(), \
                                            "' + month + '") and contains(text(), "' + year + '")]')
        file_link = file.get_attribute('href')

        cfurl = scraper.get(file_link)
        save_file_name = "mahindra_portfolios_" + d.strftime('%Y%m') + '.xlsx'

        print('Downloading file for ' + d.strftime('%b%Y'))
        with open(file_path + save_file_name, 'wb') as f:
            f.write(cfurl.content)

        driver.refresh()
        time.sleep(3)
