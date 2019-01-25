from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

SELECTOR_WAIT = 10


def find_element(driver, selector, ec_method=EC.presence_of_element_located):
    return WebDriverWait(driver, SELECTOR_WAIT).until(ec_method(selector))


def click(driver, selector):
    find_element(driver, selector, EC.element_to_be_clickable).click()


def make_checked(driver, selector):
    element = find_element(driver, selector, EC.presence_of_element_located)
    if not element.is_selected():
        element.click()


def send_keys(driver, selector, text):
    element = find_element(driver, selector, EC.visibility_of_element_located)
    element.clear()
    element.send_keys(text)
