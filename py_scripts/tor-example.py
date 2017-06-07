from selenium import webdriver

driver = webdriver.PhantomJS() # or add to your PATH
driver.set_window_size(1024, 768) # optional
driver.get('https://google.com/')
driver.save_screenshot('screen.png') # save a screenshot to disk
