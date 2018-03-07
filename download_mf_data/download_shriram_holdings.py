from selenium import webdriver
import cfscrape
import time
import os


def download(dates):
    file_path = "C:\\Users\\Administrator\\Documents\\sd-src\\sql_data\\mutual_fund_data\\raw\\shriram\\"
    chrome_driver = "C:\\Users\\Administrator\\Downloads\\chromedriver_win32\\chromedriver.exe"
    scraper = cfscrape.create_scraper()
    chrome_options = webdriver.ChromeOptions()
    prefs = {"download.default_directory": file_path}
    chrome_options.add_experimental_option("prefs", prefs)
    driver = webdriver.Chrome(executable_path=chrome_driver, chrome_options=chrome_options)
    driver.get("http://www.shriramamc.com/StatDis-MonthlyPort.aspx")

    for d in dates:
        month = d.strftime('%b')
        year = d.strftime('%Y')

        portfolio_panel = driver.find_element_by_xpath(
            './/th[contains(text(), "Monthly Portfolio for the Financial Year")]')
        portfolio_panel = portfolio_panel.find_element_by_xpath('ancestor::tbody')

        fin_year1 = year + '-' + str(int(year) + 1)
        fin_year2 = str(int(year) - 1) + '-' + year

        try:
            fin_year_panel = portfolio_panel.find_element_by_xpath('.//td[contains(text(), "' + fin_year1 + '")]')
            fin_year_panel = fin_year_panel.find_element_by_xpath('preceding-sibling::td//img')
            fin_year_panel.click()
            time.sleep(2)
            fin_year_panel = fin_year_panel.find_element_by_xpath('ancestor::tr/following-sibling::tr')
            file = fin_year_panel.find_element_by_xpath('.//td[contains(text(),"' + month + '") \
                                                                and contains(text(),"' + year + '")]')
        except:
            fin_year_panel = portfolio_panel.find_element_by_xpath('.//td[contains(text(), "' + fin_year2 + '")]')
            fin_year_panel = fin_year_panel.find_element_by_xpath('preceding-sibling::td//img')
            fin_year_panel.click()
            time.sleep(2)
            fin_year_panel = fin_year_panel.find_element_by_xpath('ancestor::tr/following-sibling::tr')
            file = fin_year_panel.find_element_by_xpath('.//td[contains(text(),"' + month + '") \
                                                                and contains(text(),"' + year + '")]')

        file.find_element_by_xpath('following-sibling::td//input').click()
        time.sleep(2)

        save_file_name = "shriram_portfolios_" + d.strftime('%Y%m') + '.xlsx'
        print('Downloading file for ' + d.strftime('%b%Y'))
        file_name = max([file_path + f for f in os.listdir(file_path)], key=os.path.getctime)
        os.rename(file_name, file_path + save_file_name)

        driver.refresh()
        time.sleep(3)

    driver.close()