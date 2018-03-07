from selenium import webdriver
import cfscrape
import time


def download(dates):
    file_path = "C:\\Users\\Administrator\\Documents\\sd-src\\sql_data\\mutual_fund_data\\raw\\ppfas\\"
    chrome_driver = "C:\\Users\\Administrator\\Downloads\\chromedriver_win32\\chromedriver.exe"
    scraper = cfscrape.create_scraper()
    driver = webdriver.Chrome(executable_path=chrome_driver)
    driver.get("https://amc.ppfas.com/schemes/portfolio-disclosure/index.php")

    for d in dates:
        month = d.strftime('%b')
        year = d.strftime('%Y')

        portfolio_panel = driver.find_element_by_xpath('.//h3[contains(text(), "Portfolio Disclosure - Parag Parikh")]')
        portfolio_panel = portfolio_panel.find_element_by_xpath('ancestor::div')

        file = portfolio_panel.find_element_by_xpath('//a[contains(text(),"' + month + '") and contains(text(),"' + year + '")]')
        file_link = file.get_attribute('href')

        cfurl = scraper.get(file_link)
        save_file_name = "ppfas_portfolios_" + d.strftime('%Y%m') + '.xlsx'

        print('Downloading file for ' + d.strftime('%b%Y'))
        with open(file_path + save_file_name, 'wb') as f:
            f.write(cfurl.content)

    driver.close()