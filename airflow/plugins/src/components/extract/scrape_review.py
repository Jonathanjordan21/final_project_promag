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



def extract_data(link, headless):#, page=1):

    # firefox_binary = FirefoxBinary()
    if link[9] == 'a':
        link = link.split("r=")[-1].split("%3F")[0].replace("%3A", ":").replace("%2F", "/")
    else :
        link = link.split("?extP")[0]

    url = f'{link}/review'
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
    browser = webdriver.Remote(f'remote_chromedriver:4444/wd/hub', options=options)

    browser.get(url)
    timeout = 6
    
    # y = 10
    # for timer in range(0,450):
    #     browser.execute_script("window.scrollTo(0, "+str(y)+")")
    #     y += 7 

    browser.execute_script("document.body.style.zoom='50%'")
    
    return browser



def scrape_page(browser):
    timeout = 15
    error_ = False
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
        "username" : "name",
        # "product":"css-kjvpvm-unf-heading e1qvo2ff8",
        "rating": "rating",
        "comment" : "css-ed1s1j-unf-heading e1qvo2ff8",
        "likes" : "css-q2y3yl",
        "date" : "css-1dfgmtm-unf-heading e1qvo2ff8",
        "variant":"css-1fp6f6k-unf-heading e1qvo2ff8"
    }

    els_dict = {x:[] for x in class_dicts.keys()}
    # els_dict['seller'] = []
    els_dict['additional_desc'] = []

    for x in browser.find_elements(By.XPATH,f"//div[@class='css-1k41fl7']"):
        # if x.get_attribute("class") == "css-1rn0irl":
        #     for y,k in zip(x.find_elements_by_tag_name("span"), ['location','seller']):
        #         els_dict[k].append(y.text)
        #     continue
        # logging.info(x.text)
        for k,v in class_dicts.items():
            # try :
            if k == "rating":
                rating_elem = EC.presence_of_element_located((By.XPATH,f".//*[@class='{v}']"))
                try :
                    els_dict[k].append(WebDriverWait(browser, 5).until(rating_elem).get_attribute('aria-label'))
                except :
                    els_dict[k].append("")
                # els_dict[k].append(x.find_element(By.XPATH,f".//*[@class='{v}']").get_attribute('aria-label'))
            elif k=="likes":
                # els_dict[k].append(x.find_element(By.XPATH,f".//*[@class='{v}']").text)
                try :
                    els_dict[k].append(x.find_element(By.XPATH,f".//*[@class='css-1ati3qk']//*[@class='{v}']").text)
                except : 
                    els_dict[k].append("")
            else :
                try :
                    els_dict[k].append(x.find_element(By.XPATH,f".//*[@class='{v}']").text)
                except:
                    els_dict[k].append("")

            # except :
            #     els_dict[k].append("")

        try :
            kendala = x.find_elements(By.XPATH,f".//*[@class='css-zhjnk4-unf-heading e1qvo2ff8']")
        
            if len(kendala) > 0:
                for k in kendala:
                    els_dict['additional_desc'].append(k.text)
            else :
                els_dict['additional_desc'].append("")
        except:
            els_dict['additional_desc'].append("")
                


    try :
        ActionChains(browser).move_to_element(
            WebDriverWait(browser, 6).until(EC.presence_of_element_located((By.CLASS_NAME, "css-16uzo3v-unf-pagination-item")))
        ).perform()
    except :
        browser.refresh()

        try :
            ActionChains(browser).move_to_element(
                WebDriverWait(browser, 6).until(EC.presence_of_element_located((By.CLASS_NAME, "css-16uzo3v-unf-pagination-item")))
            ).perform()
        except :
            logging.info("The Review only has one page")
            error_ = True
            return browser,els_dict,error_

    buttons = browser.find_elements(By.CLASS_NAME, "css-16uzo3v-unf-pagination-item")
    # len_btn = len(buttons)
    for page in buttons:
        try :
            if page.get_attribute('aria-label') == "Laman berikutnya":
                page.click()
                logging.info("Found!, clicked!")
                break
            else :
                if len(buttons) == 1:
                    logging.info("All data has been sucessfully retrieved!")
                    return browser,els_dict
        except:
            logging.info("Error Occur! page text " + str(page.text))

    list_class_w = "css-1k41fl7"
    list_wait = EC.presence_of_element_located((By.CLASS_NAME, list_class_w))

    try :
        WebDriverWait(browser, 10).until(list_wait)
    except :
        browser.refresh()
        WebDriverWait(browser, 10).until(list_wait)

    time.sleep(0.7)
    # y = 1000
    # for timer in range(0,4):
    #     browser.execute_script("window.scrollTo(0, "+str(y)+")")
    #     y += 1000  
    #     time.sleep(0.75)
    error_ = False
    return browser,els_dict,error_

def run(link, jumlah_halaman, headless=False):
    logging.info("Initializing...")
    data = pd.DataFrame()
    if link[9] == 'a':
        link = link.split("r=")[-1].split("%3F")[0].replace("%3A", ":").replace("%2F", "/")
    else :
        link = link.split("?extP")[0]
    # link = "https://www.tokopedia.com/asus/asus-vivobook-a416mao-fhd426-slate-grey?extParam=ivf%3Dfalse%26src%3Dsearch%26whid%3D7377294"
    try :
        driver = extract_data(link,headless)
        logging.info("Start Scrapping")
        for n in range(1,jumlah_halaman+1):
            driver, data_dict, error_ = scrape_page(driver)
            data = pd.concat([data,pd.DataFrame(data_dict)])
            logging.info("Scrapping page " + str(n) + " success!")
            if error_:
                break
        data['likes'] = data['likes'].map(lambda x : 0 if x == "Membantu" else x.split(" ")[0])#.astype('int')
        seller, product =  link.split("/")[3:5]
        data['product'] = product.replace("-", " ")
        data['seller'] = seller
        data['link'] = link
        driver.quit()
        return data
        
        # data.to_csv(f"laptop-review-tokped.csv", index=False)
    except Exception as err:
        try :
            raise CustomException(err,sys)
        except :
            if len(data) > 0:
                data['likes'] = data['likes'].map(lambda x : 0 if x[0] == "M" else x.split(" ")[0])#.astype('int')
                seller, product =  link.split("/")[3:5]
                data['product'] = product.replace("-", " ")
                data['seller'] = seller
                data['link'] = link
            
            return data
    #         pass
    # return data



if __name__ == "__main__":
    logging.info("Initializing...")
    data = pd.DataFrame()
    link = "https://www.tokopedia.com/asus/asus-vivobook-a416mao-fhd426-slate-grey?extParam=ivf%3Dfalse%26src%3Dsearch%26whid%3D7377294"
    driver = extract_data(link)
    logging.info("Start Scrapping")
    for n in range(1,6):
        driver, data_dict = scrape_page(driver)
        data = pd.concat([data,pd.DataFrame(data_dict)])
        logging.info(data)
        logging.info("Scrapping page", n, "success!")
    data['likes'] = data['likes'].map(lambda x : 0 if x == "Membantu" else x.split(" ")[0]).astype('int')
    data.to_csv(f"laptop-review-tokped.csv", index=False)

# content = browser.find_element_by_xpath(name_xpath).text
# logging.info(content)