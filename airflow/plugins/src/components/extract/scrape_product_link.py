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

import time,sys,os,re
from datetime import datetime

from src.logger import logging
from src.exception import CustomException

import pandas as pd


def extract_data(link, headless, browser=None):#, page=1):

    # firefox_binary = FirefoxBinary()
    # url = f'https://www.tokopedia.com/{username_toko}/review'
    # driver = 'geckodriver.exe'
    # driver = 'C:\\Users\\jonat\\Downloads\\geckodriver-v0.32.2-win32\\geckodriver.exe'
    # browser = webdriver.Firefox(firefox_binary=firefox_binary,executable_path=driver)
    if browser == None:
        options = Options()
        options.add_argument("--disable-javascript")
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        if headless:
            options.add_argument('--headless')
            user_agent = 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.50 Safari/537.36'
            options.add_argument(f'user-agent={user_agent}')
        # options.set_capability("marionette", True )
        # browser = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
        browser = webdriver.Remote("remote_chromedriver:4444/wd/hub", options=options)
    # html = browser.find_element(By.TAG_NAME, "html")
    # for _ in range(5):
    #     html.send_keys(Keys.CONTROL + "-")


    # browser.get(link+"/review")
    logging.info(link)
    browser.get(link)
    # if browser == None:
    browser.execute_script("document.body.style.zoom='25%'")
    # try :
    #     el = EC.presence_of_element_located((By.XPATH,"//div[@class='css-124sv7s undefined']"))#"//div[@id='pdp_comp-review']/div/div/div/div"))
    #     el = WebDriverWait(browser, 6).until(el)
    #     el = el.text
    #     print("get")

    #     split = el.split('\n')
    # except :
    #     split = ""
    # try :
    #     puas = split[2].split(" ")[0]
    # except:
    #     puas = ""
    # try :
    #     rating, ulasan = (x.strip().split(" ")[0] for x in split[3].split("•"))
    # except:
    #     rating=""
    #     ulasan=""
    
    # try :
    #     ratings = split[7::3]
    # except:
    #     ratings = ""

    # if len(ratings) == 0:
    #     ratings = [0]*5
    
    try :
        el = EC.presence_of_element_located((By.XPATH,"//*[contains(@class,'eytdjj02')]"))#"//div[@id='pdp_comp-review']/div/div/div/div"))
        el = WebDriverWait(browser, 6).until(el)
    
        detail = el.text#.split("Kondisi:")[-1].split("\n")[0].strip()
        # Define the pattern using a regular expression
        pattern = r'Kondisi: (.*?)\n'

        # Use re.findall to find all matches in the text
        detail = re.findall(pattern, detail, re.DOTALL)[0]
    except:
        detail = ""


    it = browser.find_elements(By.XPATH, "//div[@class='items']/p")
    sold =0
    seller_rating =0
    resp_time =0
    rates = True
    for x_ in it:
        if x_.get_attribute('data-testid') == 'lblPDPDetailProductSoldCounter':
            sold = x_.text[8:].replace(' rb','000')
            try :
                sold = int(sold) if sold[-1]!='+' else int(sold[:-1]) + int(datetime.now().second%11)
            except :
                sold = int(x_.text.split(" ")[0])
                rates=False
    
    it = browser.find_elements(By.XPATH, "//div[@id='pdp_comp-shop_credibility']/div[2]/div[2]/div")

    for x_ in it:
        if x_.get_attribute('data-testid') == 'lblPDPShopPackFirst':
            seller_rating = float(x_.text.split("r")[0])
        else :
            a_ = x_.text.split(" ")
            try :
                resp_time = int(a_[1])
                a_ = a_[2].split("p")
                if 'ri' == a_[0][-2:]:
                    resp_time = 24*60*resp_time
                elif 'am' == a_[0][-2:]:
                    resp_time = 60 * resp_time
            except :
                resp_time = ""
    

    if rates and sold > 0:
        el = EC.presence_of_element_located((By.XPATH,"//div[@class='css-124sv7s undefined']"))#"//div[@id='pdp_comp-review']/div/div/div/div"))
        el = WebDriverWait(browser, 8).until(el)
        el = el.text
        print("get")

        split = el.split('\n')
        try :
            puas = split[2].split(" ")[0]
        except:
            puas = ""
        try :
            rating, ulasan = (x.strip().split(" ")[0] for x in split[3].split("•"))
        except:
            rating=""
            ulasan=""
        
        try :
            ratings = split[7::3]
        except:
            ratings = [0,0,0,0,0]
    else :
        puas = ""
        ratings = [0,0,0,0,0]
        rating = ""
        ulasan = ""



    return browser, {k:v for k,v in zip(["kepuasan","total_rating","total_comment", "kondisi", "sold","seller_rating","menit_kecepatan_pengemasan"]+[f"bintang {i}" for i in range(5,0,-1)], [puas,rating,ulasan,detail,sold,seller_rating,resp_time]+ratings)}


# link = 'https://www.tokopedia.com/sinmob/laptop-advan-soulmate-4-128gb-intel-celeron-n4020-garansi-resmi-promo-grey?extParam=cmp%3D1%26ivf%3Dfalse%26src%3Dsearch'

# links = pd.read_csv(os.path.join('data','data-tokped.csv')).link

# s = time.time()
# data = []
# driver = None
# for link in links[:100]:
#     try :
#         if link[9] == 'a':
#             link = link.split("r=")[-1].split("%3F")[0].replace("%3A", ":").replace("%2F", "/")
#         else :
#             link = link.split("?extP")[0]
        
#         driver, d = extract_data(link+"/review", False, driver)
#         print(d)
#         data.append(d)
#         # print(data)
#     except Exception as e:
#         print("ERROR!", e)
#         print(link)
# # print(type(a))
# data = pd.DataFrame(data)
# data.to_csv('test.csv')
# print(time.time() - s)

def run(link, headless, driver=None):
    logging.info('Initializing....')
    
    data = []
    if link[9] == 'a':
        link = link.split("r=")[-1].split("%3F")[0].replace("%3A", ":").replace("%2F", "/")
    else :
        link = link.split("?extP")[0]
    try :
        logging.info('Start Scrapping...')
        driver, d = extract_data(link, headless, driver)
        data.append(d)
        logging.info("Scrapping link success!")
        data = pd.DataFrame(data)
        seller, product =  link.split("/")[3:5]
        data['product'] = product.replace("-", " ")
        data['seller'] = seller
        data['link'] = link
        return driver,data
    except Exception as err:
        print(link)
        driver.quit()
        raise CustomException(err,sys)
    
    # data.to_csv('test.csv')
    return driver, data
