from selenium import webdriver
import cfscrape
import time


def download(dates):
    file_path = "C:\\Users\\Administrator\\Documents\\sd-src\\sql_data\\mutual_fund_data\\raw\\union\\"
    chrome_driver = "C:\\Users\\Administrator\\Downloads\\chromedriver_win32\\chromedriver.exe"
    scraper = cfscrape.create_scraper()
    driver = webdriver.Chrome(executable_path=chrome_driver)
    driver.get("http://www.unionmf.com/downloads/others/monthlyportfolios.aspx")

    year_panel = driver.find_element_by_xpath('.//span[contains(text(), "Select Year")]')
    year_panel = year_panel.find_element_by_xpath('following-sibling::div//select')

    for d in dates:
        month = d.strftime('%b')
        year = d.strftime('%Y')

        year_panel.find_element_by_xpath('./option[text() = "' + year + '"]').click()
        time.sleep(2)

        file = driver.find_element_by_xpath('.//a[contains(text(),"' + month + '") and contains(text(),"' + year + '")]')
        file_link = file.get_attribute('href')

        cfurl = scraper.get(file_link)
        save_file_name = "union_portfolios_" + d.strftime('%Y%m') + '.xlsx'

        print('Downloading file for ' + d.strftime('%b%Y'))
        with open(file_path + save_file_name, 'wb') as f:
            f.write(cfurl.content)
