from selenium import webdriver
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities

# Set the desired capabilities
capabilities = DesiredCapabilities.CHROME.copy()
capabilities['chromeOptions'] = {'debuggerAddress': 'localhost:9222'}

# Create a new browser instance with the desired capabilities
browser = webdriver.Chrome(desired_capabilities=capabilities)

# The browser session is now resumed
