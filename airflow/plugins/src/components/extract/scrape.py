from selenium import webdriver
from selenium.webdriver.firefox.firefox_binary import FirefoxBinary
from selenium.common.exceptions import TimeoutException
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.action_chains import ActionChains
# from webdriver_manager.firefox import GeckoDriverManager
# from selenium.webdriver.firefox.service import Service
# from selenium.webdriver.firefox.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options

from src.logger import logging
from src.exception import CustomException
import time,sys


import pandas as pd
# SHEET_URL = "https://docs.google.com/spreadsheets/d/e/2PACX-1vQKedddvke65kn6omUuSdG6DTfHK9M5L_m1GHBRd-ui2VGx5drxVtH5BlFu3701ruu9P6EuWgmgIseJ/pub?gid=0&single=true&output=csv"
# df = pd.read_csv(SHEET_URL)


# linkedin = df['LinkedIn'].str.strip()
# linkedin.dropna(inplace=True)



def extract_data(keyword, headless):

    # firefox_binary = FirefoxBinary()
    url = f'https://www.tokopedia.com/find/{keyword}'
    # driver = 'geckodriver.exe'
    # driver = 'C:\\Users\\jonat\\Downloads\\geckodriver-v0.32.2-win32\\geckodriver.exe'
    # browser = webdriver.Firefox(firefox_binary=firefox_binary,executable_path=driver)
    options = Options()
    options.add_argument("--disable-javascript")
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    if headless:
        options.add_argument('--headless')
        user_agent = 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.50 Safari/537.36'
        options.add_argument(f'user-agent={user_agent}')
    # options.set_capability("marionette", True )
    # browser = webdriver.Firefox(service=Service(GeckoDriverManager().install()), options=options)
    # browser = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)

    browser = webdriver.Remote(f'remote_chromedriver:4444/wd/hub', options=options)

    browser.get(url)
    timeout = 10

    # search_xpath = "/html[1]/body[1]/div[1]/div[1]/div[1]/div[1]/div[1]/div[2]/div[2]/div[1]/div[1]/div[1]/div[1]/input[1]"

    # search_wait = EC.presence_of_element_located((By.XPATH, search_xpath))
    # WebDriverWait(browser, timeout).until(search_wait)
    # # input_user = input("Mau cari barang apa? ")
    # # input_user = "turtleneck"
    # browser.find_element(By.XPATH,search_xpath).send_keys(keyword)
    # browser.find_element(By.XPATH,search_xpath).send_keys(Keys.ENTER)
    
    y = 10
    for timer in range(0,750):
        browser.execute_script("window.scrollTo(0, "+str(y)+")")
        y += 10 
    
    i = 0

    try :
        ActionChains(browser).move_to_element(
            WebDriverWait(browser, timeout).until(EC.presence_of_element_located((By.CLASS_NAME, "css-1ni9y5x-unf-pagination-items")))
        ).perform()
    except:
        browser.refresh()
        ActionChains(browser).move_to_element(
            WebDriverWait(browser, timeout).until(EC.presence_of_element_located((By.CLASS_NAME, "css-1ni9y5x-unf-pagination-items")))
        ).perform()


    
    for page in browser.find_elements(By.CLASS_NAME, "css-bugrro-unf-pagination-item"):
        try :
            i = max(i,int(page.text.replace(".","")))
        except:
            logging.info("Error Occur! page text " + str(page.text))
            # time.sleep(1)
    
    return browser, i



def scrape_page(browser, next_page):
    timeout = 10
    
    class_dicts = {
        # "name" : "prd_link-product-name css-3um8ox",
        ## "price" : "prd_link-product-price css-1ksb19c",
        # "price": "prd_link-product-price css-h66vau",
        # "cashback(%)" : "prd_label-product-price css-tolj34",
        # "seller" : "prd_link-shop-name css-1kdc32b flip",
        # "location" : "prd_link-shop-loc css-1kdc32b flip",
        # "rating" : "prd_rating-average-text css-t70v7i",
        # # "sold" : "prd_label-integrity css-1duhs3e",
        # "sold" : "prd_label-integrity css-1sgek4h",
        # "price" : "prd_link-product-price css-1ksb19c",
        "name" : "prd_link-product-name",
        "price": "prd_link-product-price",
        "cashback(%)" : "prd_label-product-price",
        "seller" : "prd_link-shop-name",
        "location" : "prd_link-shop-loc",
        "rating" : "prd_rating-average-text",
        "sold" : "prd_label-integrity",
        "status" : {"css-1o5lmrf":"Pro Merchant", "css-1hkzhs1":"Official Store", "css-xsuazh":"Power Merchant"},
        "ads":"css-147fo5b"


        # "shop_tag_cls" : "css-1ktbh56",
        # "tag_pro_cls" : "css-1o5lmrf",

    }

    els_dict = {x:[] for x in class_dicts.keys()}
    els_dict['link'] = []
    els_dict['image'] = []

    for x in browser.find_elements(By.XPATH, "//div[contains(@class,'prd_container-card')]"):
        # if x.get_attribute("class") == "css-1rn0irl":
        #     for y,k in zip(x.find_elements_by_tag_name("span"), ['location','seller']):
        #         els_dict[k].append(y.text)
        #     continue
        # logging.info(x.text)
        for k,v in class_dicts.items():
            try :
                if k == "status":
                    els_dict[k].append(v[x.find_element(By.XPATH,".//*[@class='css-z1kcla']/i").get_attribute('class')])
                else :
                    # els_dict[k].append(x.find_element(By.XPATH,f".//*[@class='{v}']").text)
                    els_dict[k].append(x.find_element(By.XPATH,f".//*[contains(@class, '{v}')]").text)

            except :
                els_dict[k].append("")

        # els_dict['link'].append(x.find_element(By.XPATH, ".//*[@class='css-1f2quy8']/a").get_attribute('href'))
        els_dict['link'].append(x.find_element(By.XPATH, "./div/div/a").get_attribute('href'))
        # els_dict['image'].append(x.find_element(By.XPATH, ".//*[@class='css-1q90pod']").get_attribute('src'))
        els_dict['image'].append(x.find_element(By.XPATH, ".//*[contains(@class,'pcv3_img_container')]/img").get_attribute('src'))
    
    # logging.info()

    ActionChains(browser).move_to_element(
        WebDriverWait(browser, timeout).until(EC.presence_of_element_located((By.CLASS_NAME, "css-1ni9y5x-unf-pagination-items")))
    ).perform()


    for page in browser.find_elements(By.CLASS_NAME,  "css-bugrro-unf-pagination-item"):
        try :
            if next_page == int(page.text):
                page.click()
                break
        except:
            # if page.text == 
            logging.info("Error Occur! page text " + str(page.text))


    list_class = "/html[1]/body[1]/div[1]/div[1]/div[2]/div[1]/div[2]/div[4]"
    list_class_w = "css-1q90pod"
    list_wait = EC.presence_of_element_located((By.CLASS_NAME, list_class_w))
    try :
        WebDriverWait(browser, timeout).until(list_wait)
    except:
        browser.refresh()
        WebDriverWait(browser, timeout).until(list_wait)


    # y = 10
    # for timer in range(0,750):
    #     browser.execute_script("window.scrollTo(0, "+str(y)+")")
    #     y += 10 
    y = 1000
    for timer in range(0,6):
        browser.execute_script("window.scrollTo(0, "+str(y)+")")
        y += 1000  
        time.sleep(0.75)
    return browser,els_dict


    

        
    

# browser.execute_script("window.scrollTo(0,document.body.scrollHeight)")
# time.sleep(timeout//3)
# browser.execute_script("window.scrollTo(0,document.body.scrollHeight)")
# time.sleep(timeout//3)
# browser.execute_script("window.scrollTo(0,document.body.scrollHeight)")
# time.sleep(timeout//3)
    # content = browser.find_element_by_xpath(list_class)
# row_list = content.find_elements_by_xpath('./div')
# # for row in row_list:
# #     for item in row.find_elements_by_xpath('./div'):
# #         for item_elements in item.find_elements_by_xpath('./div[1]/div[1]/div[1]/div[1]/div[1]/div'):
# #             logging.info(item_elements.get_attribute("class"))
# for row in row_list:
#     for item in row.find_elements_by_class_name('pcv3__info-content css-gwkf0u'):
#         logging.info(item.get_attribute("class"))

    

    
# ll = {seller}

    
        

        # if x.get_attribute('class') == v:
        #     df[k] = x.text
        # if y.get_attribute('class') == name_cls:
        #     df['name'] = y.text
        # elif y.get_attribute('class') == price_cls:
        #     df['price'] = y.text
        # elif y.get_attribute('class') == seller_cls:
        #     df['seller'] = y.text
        # elif y.get_attribute('class') == price_cls:
        #     df['price'] = y.text

def load_data(els_dict):
    df = pd.DataFrame(els_dict)
    df['sold'] = df['sold'].apply(lambda x:int(x.replace("+","").replace(" terjual","").replace("rb", "000")))
    df['ads'] = df['ads'].apply(lambda x:True if x=='Ad' else False)
    # df['cashback(%)'] = df['cashback(%)'].apply(lambda x:float(x.replace("Cashback ", "").replace("%", "")))
    df.to_csv("file.csv", index=False)
    return df



# for el in els:
#     # logging.info(el.get_attribute("id"))
#     logging.info(el.get_attribute("class"))
    # logging.info(el.text)


def run(nama_produk, jumlah_halaman, headless=False):
    logging.info("Initializing...")
    data = pd.DataFrame()
    try :
        driver, max_page = extract_data(nama_produk, headless)
        logging.info("Start Scrapping")
        for n in range(1,jumlah_halaman+1):
            driver, data_dict = scrape_page(driver, n+1)
            # data_dict['page'] = [n]*len(data_dict[])
            data_dict = pd.DataFrame(data_dict)
            data_dict['page'] = n
            data = pd.concat([data,data_dict])
            logging.info("Scrapping page " + str(n) + " success!")
        driver.quit()
        return data
        # data.to_csv(f"{nama_produk}.csv", index=False)
        # return f"{nama_produk}.csv"
    except Exception as err:
        raise CustomException(err,sys)
        
    return data

if __name__ == "__main__":
    logging.info("Initializing...")
    data = pd.DataFrame()
    driver, max_page = extract_data("laptop")
    logging.info("Start Scrapping")
    for n in range(1,3):
        driver, data_dict = scrape_page(driver, n+1)
        data = pd.concat([data,pd.DataFrame(data_dict)])
        logging.info("Scrapping page", n, "success!")
    data.to_csv("data-tokped.csv", index=False)

# content = browser.find_element_by_xpath(name_xpath).text
# logging.info(content)