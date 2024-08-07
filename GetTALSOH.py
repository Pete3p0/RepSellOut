import streamlit as st
import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
import logging
from datetime import datetime
from io import BytesIO

# Configure logging
logging.basicConfig(level=logging.DEBUG)

# Setup Streamlit interface
st.title("Takealot Data Scraper")

uploaded_file = st.file_uploader("Choose an Excel file", type="xlsx")

if uploaded_file is not None:
    try:
        df = pd.read_excel(uploaded_file, engine='openpyxl')
        st.write("Excel file read successfully")
        st.write(df)
    except Exception as e:
        st.error(f"Error reading Excel file: {e}")

    def scroll_into_view(element, driver):
        driver.execute_script("arguments[0].scrollIntoView({block: 'center', inline: 'center'});", element)

    def close_banner_and_add_to_cart(driver):
        try:
            # Close any potential cookie banner
            cookie_banner = WebDriverWait(driver, 5).until(
                EC.element_to_be_clickable((By.CLASS_NAME, 'cookies-banner-module_cookie-banner_hsodu'))
            )
            cookie_banner.click()
            logging.debug("Cookie banner closed")
        except:
            logging.debug("No cookie banner found")
        
        try:
            # Find and click the correct Add to Cart button
            add_to_cart_button = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, '.action-cart .add-to-cart-button-module_add-to-cart-button_1a9gT'))
            )
            scroll_into_view(add_to_cart_button, driver)
            add_to_cart_button.click()
            logging.debug("Add to Cart button clicked")
        except Exception as e:
            logging.error(f"Error clicking Add to Cart button: {e}")

    # Get today's date
    today_date = datetime.now().strftime("%Y-%m-%d")

    # Iterate over each URL in the DataFrame
    for index, row in df.iterrows():
        url = row['Item']  # Assuming URLs are in column 'Item'
        logging.debug(f"Processing URL: {url}")
        
        # Setup Selenium with Chrome in incognito mode for each URL
        chrome_options = Options()
        chrome_options.add_argument("--incognito")
        chrome_options.add_argument("--headless")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--no-sandbox")
        
        try:
            driver = webdriver.Chrome(ChromeDriverManager().install(), options=chrome_options)
            logging.debug("WebDriver initialized successfully")
        except Exception as e:
            logging.error(f"Error initializing WebDriver: {e}")
            continue
        
        try:
            driver.get(url)
            logging.debug(f"Navigating to URL: {url}")
            close_banner_and_add_to_cart(driver)
            
            # Navigate to cart and set quantity
            driver.get("https://www.takealot.com/cart")
            logging.debug("Navigating to cart page")
            
            try:
                quantity_dropdown = WebDriverWait(driver, 10).until(
                    EC.element_to_be_clickable((By.CSS_SELECTOR, 'select[data-ref="cart-item_undefined"]'))
                )
                quantity_dropdown.click()
                quantity_dropdown_option = WebDriverWait(driver, 10).until(
                    EC.element_to_be_clickable((By.XPATH, '//select[@data-ref="cart-item_undefined"]/option[@value="10"]'))
                )
                quantity_dropdown_option.click()
                logging.debug("Selected 10+ option from quantity dropdown")
                
                # Wait until the quantity input field is interactable
                quantity_input = WebDriverWait(driver, 10).until(
                    EC.element_to_be_clickable((By.CSS_SELECTOR, 'input#cart-item_undefined'))
                )
                quantity_input.clear()
                quantity_input.send_keys('100000')
                logging.debug("Entered quantity 100000")
                
                # Find and click the Update button
                update_button = WebDriverWait(driver, 10).until(
                    EC.element_to_be_clickable((By.CSS_SELECTOR, 'button[data-ref="quantity-update-button"]'))
                )
                scroll_into_view(update_button, driver)
                update_button.click()
                logging.debug("Clicked Update button")
                
                # Capture the actual available quantity if there is an error message
                try:
                    error_message = WebDriverWait(driver, 10).until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, 'div.message.alert-banner-module_message_2sinO span'))
                    )
                    available_quantity_text = error_message.text
                    available_quantity = int(available_quantity_text.split(' ')[-2])
                    logging.debug(f"Available quantity captured: {available_quantity}")
                    
                    # Add the available quantity to the DataFrame
                    if today_date not in df.columns:
                        df[today_date] = ""
                    df.at[index, today_date] = available_quantity

                except Exception as e:
                    logging.debug("No error message found, quantity update successful")
                
            except Exception as e:
                logging.error(f"Error updating quantity: {e}")
        except Exception as e:
            logging.error(f"An error occurred while processing URL {url}: {e}")
        finally:
            driver.quit()
            logging.debug("WebDriver session ended")

    st.write("Updated DataFrame")
    st.write(df)

    # Save the updated DataFrame back to an Excel file
    def to_excel(df):
        output = BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            df.to_excel(writer, index=False, sheet_name='Sheet1')
        processed_data = output.getvalue()
        return processed_data

    updated_file = to_excel(df)
    
    st.download_button(
        label="Download updated Excel file",
        data=updated_file,
        file_name=f"updated_data_{today_date}.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )
