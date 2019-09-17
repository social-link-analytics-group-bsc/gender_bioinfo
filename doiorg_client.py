from selenium import webdriver

import logging
import pathlib
import random
import time

logging.basicConfig(filename=str(pathlib.Path(__file__).parents[0].joinpath('gender_identification.log')),
                    level=logging.DEBUG)


class DoiClient:
    driver = None

    def __init__(self):
        self.driver = webdriver.Chrome()

    def __is_robot_page(self):
        try:
            robot_element = self.driver.find_element_by_xpath("//button[@id='btnSubmit']")
            return robot_element and robot_element.text.lower() == 'take me to my content'
        except:
            return False

    def __process_robot_page(self, doi_link):
        while True:
            time_to_sleep = random.randint(200, 300)
            logging.info(f"Going to sleep for {time_to_sleep} seconds")
            time.sleep(time_to_sleep)
            # after waiting some time, try again
            self.driver.get("https://dx.doi.org/")
            element = self.driver.find_element_by_xpath("//input[@name='hdl'][@type='text']")
            element.send_keys(str(doi_link))
            element.submit()
            if self.__is_robot_page():
                continue
            if 'unavailable' in self.driver.current_url:
                time.sleep(2)
                return None
            else:
                time_to_sleep = random.randint(5, 10)
                time.sleep(time_to_sleep)
                return self.driver.current_url

    def get_paper_link_from_doi(self, paper_doi):
        self.driver.get("https://dx.doi.org/")
        element = self.driver.find_element_by_xpath("//input[@name='hdl'][@type='text']")
        element.send_keys(str(paper_doi))
        element.submit()
        if 'unavailable' in self.driver.current_url:
            # page not found...
            logging.info('Page not found!')
            return None
        elif self.__is_robot_page():
            # we are detected as robots...
            logging.info('We were detected as robot :(')
            return self.__process_robot_page(paper_doi)
        else:
            return self.driver.current_url