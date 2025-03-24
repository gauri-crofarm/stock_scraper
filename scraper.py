import streamlit as st
import pandas as pd
import time
import os
import base64
import threading
import sys
import io
from contextlib import redirect_stdout, redirect_stderr
import yfinance as yf
import logging
from datetime import datetime

# Set page configuration
st.set_page_config(
    page_title="Stock Data Scraper",
    page_icon="📊",
    layout="wide"
)

# Function to capture stdout/stderr
class OutputCapture:
    def __init__(self):
        self.output = io.StringIO()

    def __enter__(self):
        self.old_stdout = sys.stdout
        self.old_stderr = sys.stderr
        sys.stdout = self.output
        sys.stderr = self.output
        return self

    def __exit__(self, *args):
        sys.stdout = self.old_stdout
        sys.stderr = self.old_stderr

    def get_output(self):
        return self.output.getvalue()


# Setup logging to a string buffer
def setup_logging(ticker_name):
    log_handler = logging.StreamHandler(io.StringIO())
    log_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    log_handler.setFormatter(log_formatter)

    logger = logging.getLogger(f"stock_scraper_{ticker_name}")
    logger.setLevel(logging.INFO)
    logger.addHandler(log_handler)

    return logger, log_handler

# Create output directory
def create_output_directory(ticker_symbol):
    output_dir = f"stock_data_{ticker_symbol.replace('.', '_')}"
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    return output_dir

# Save data function
def save_data(data, filename, output_dir, logger):
    filepath = os.path.join(output_dir, filename)

    # Check if data is empty before saving
    if data is None or (isinstance(data, pd.DataFrame) and data.empty):
        logger.warning(f"Skipping save for {filename}: Data is empty")
        return False

    try:
        if isinstance(data, pd.DataFrame):
            data.to_csv(filepath, index=True)
            logger.info(f"Saved DataFrame to {filepath} with {len(data)} rows")
            return True
        elif isinstance(data, dict):
            df = pd.DataFrame.from_dict(data, orient='index', columns=['Value'])
            if not df.empty:
                df.to_csv(filepath)
                logger.info(f"Saved dict to {filepath} with {len(df)} rows")
                return True
            else:
                logger.warning(f"Skipping save for {filename}: Dict converted to empty DataFrame")
                return False
        else:
            content = str(data)
            if content.strip():
                with open(filepath, 'w') as f:
                    f.write(content)
                logger.info(f"Saved data to {filepath} ({len(content)} characters)")
                return True
            else:
                logger.warning(f"Skipping save for {filename}: Empty string")
                return False
    except Exception as e:
        logger.error(f"Failed to save data to {filepath}: {str(e)}")
        return False


# Get stock data function
def get_stock_data(ticker_symbol, ticker_name, retry_count=3, delay=2, logger=None):
    if logger is None:
        logger, _ = setup_logging(ticker_name)

    logger.info(f"Fetching data for {ticker_name} ({ticker_symbol})")
    successful_files = 0
    generated_files = []
    summary_report_path = None

    try:
        # Create Ticker object with retry mechanism
        ticker = None
        for attempt in range(retry_count):
            try:
                ticker = yf.Ticker(ticker_symbol)
                # Quick validation to check if the ticker is valid
                _ = ticker.info.get('shortName', None)
                logger.info(f"Successfully created Ticker object for {ticker_symbol}")
                break
            except Exception as e:
                if attempt < retry_count - 1:
                    logger.warning(f"Attempt {attempt + 1}/{retry_count} failed: {str(e)}. Retrying...")
                    time.sleep(delay)
                else:
                    logger.error(f"Failed to create Ticker object after {retry_count} attempts: {str(e)}")
                    return False, [], None

        if ticker is None:
            return False, [], None

        output_dir = create_output_directory(ticker_symbol)

        # 1. Basic company info
        logger.info("Fetching company information")
        try:
            info = ticker.info
            if info and len(info) > 0:
                filename = "company_info.csv"
                if save_data(pd.DataFrame.from_dict(info, orient='index', columns=['Value']),
                             filename, output_dir, logger):
                    successful_files += 1
                    generated_files.append(os.path.join(output_dir, filename))
        except Exception as e:
            logger.warning(f"Failed to fetch company information: {str(e)}")

        # 2. Historical market data
        logger.info("Fetching historical market data (max period)")
        try:
            hist = ticker.history(period="max")
            if not hist.empty:
                filename = "historical_data.csv"
                if save_data(hist, filename, output_dir, logger):
                    successful_files += 1
                    generated_files.append(os.path.join(output_dir, filename))
        except Exception as e:
            logger.warning(f"Failed to fetch historical data: {str(e)}")

        # 3. Financials
        logger.info("Fetching income statement")
        try:
            income_stmt = ticker.income_stmt
            if not income_stmt.empty:
                filename = "income_statement.csv"
                if save_data(income_stmt, filename, output_dir, logger):
                    successful_files += 1
                    generated_files.append(os.path.join(output_dir, filename))
        except Exception as e:
            logger.warning(f"Failed to fetch income statement: {str(e)}")

        logger.info("Fetching balance sheet")
        try:
            balance_sheet = ticker.balance_sheet
            if not balance_sheet.empty:
                filename = "balance_sheet.csv"
                if save_data(balance_sheet, filename, output_dir, logger):
                    successful_files += 1
                    generated_files.append(os.path.join(output_dir, filename))
        except Exception as e:
            logger.warning(f"Failed to fetch balance sheet: {str(e)}")

        logger.info("Fetching cash flow")
        try:
            cash_flow = ticker.cashflow
            if not cash_flow.empty:
                filename = "cash_flow.csv"
                if save_data(cash_flow, filename, output_dir, logger):
                    successful_files += 1
                    generated_files.append(os.path.join(output_dir, filename))
        except Exception as e:
            logger.warning(f"Failed to fetch cash flow: {str(e)}")

        # 4. Quarterly financials
        logger.info("Fetching quarterly income statement")
        try:
            quarterly_income = ticker.quarterly_income_stmt
            if not quarterly_income.empty:
                filename = "quarterly_income_statement.csv"
                if save_data(quarterly_income, filename, output_dir, logger):
                    successful_files += 1
                    generated_files.append(os.path.join(output_dir, filename))
        except Exception as e:
            logger.warning(f"Failed to fetch quarterly income statement: {str(e)}")

        logger.info("Fetching quarterly balance sheet")
        try:
            quarterly_balance = ticker.quarterly_balance_sheet
            if not quarterly_balance.empty:
                filename = "quarterly_balance_sheet.csv"
                if save_data(quarterly_balance, filename, output_dir, logger):
                    successful_files += 1
                    generated_files.append(os.path.join(output_dir, filename))
        except Exception as e:
            logger.warning(f"Failed to fetch quarterly balance sheet: {str(e)}")

        logger.info("Fetching quarterly cash flow")
        try:
            quarterly_cashflow = ticker.quarterly_cashflow
            if not quarterly_cashflow.empty:
                filename = "quarterly_cash_flow.csv"
                if save_data(quarterly_cashflow, filename, output_dir, logger):
                    successful_files += 1
                    generated_files.append(os.path.join(output_dir, filename))
        except Exception as e:
            logger.warning(f"Failed to fetch quarterly cash flow: {str(e)}")

        # 5. Major shareholders
        logger.info("Fetching major shareholders")
        try:
            major_holders = ticker.major_holders
            if major_holders is not None and not major_holders.empty:
                filename = "major_holders.csv"
                if save_data(major_holders, filename, output_dir, logger):
                    successful_files += 1
                    generated_files.append(os.path.join(output_dir, filename))
        except Exception as e:
            logger.warning(f"Failed to fetch major holders: {str(e)}")

        # 6. Analyst recommendations
        logger.info("Fetching analyst recommendations")
        try:
            recommendations = ticker.recommendations
            if recommendations is not None and not recommendations.empty:
                filename = "recommendations.csv"
                if save_data(recommendations, filename, output_dir, logger):
                    successful_files += 1
                    generated_files.append(os.path.join(output_dir, filename))
        except Exception as e:
            logger.warning(f"Failed to fetch recommendations: {str(e)}")

        # 7. ESG data
        logger.info("Fetching ESG data")
        try:
            esg_data = ticker.sustainability
            if esg_data is not None and not esg_data.empty:
                filename = "esg_data.csv"
                if save_data(esg_data, filename, output_dir, logger):
                    successful_files += 1
                    generated_files.append(os.path.join(output_dir, filename))
        except Exception as e:
            logger.warning(f"Failed to fetch ESG data: {str(e)}")

        # 8. News articles
        logger.info("Fetching news articles")
        try:
            news = ticker.news
            if news and len(news) > 0:
                filename = "news.csv"
                news_df = pd.DataFrame(news)
                if save_data(news_df, filename, output_dir, logger):
                    successful_files += 1
                    generated_files.append(os.path.join(output_dir, filename))
        except Exception as e:
            logger.warning(f"Failed to fetch news: {str(e)}")

        # 9. Actions (dividends, splits)
        logger.info("Fetching actions (dividends, splits)")
        try:
            actions = ticker.actions
            if actions is not None and not actions.empty:
                filename = "actions.csv"
                if save_data(actions, filename, output_dir, logger):
                    successful_files += 1
                    generated_files.append(os.path.join(output_dir, filename))
        except Exception as e:
            logger.warning(f"Failed to fetch actions: {str(e)}")

        # 10. Dividends
        logger.info("Fetching dividends")
        try:
            dividends = ticker.dividends
            if dividends is not None and len(dividends) > 0:
                filename = "dividends.csv"
                if save_data(pd.DataFrame(dividends), filename, output_dir, logger):
                    successful_files += 1
                    generated_files.append(os.path.join(output_dir, filename))
        except Exception as e:
            logger.warning(f"Failed to fetch dividends: {str(e)}")

        # 11. Splits
        logger.info("Fetching splits")
        try:
            splits = ticker.splits
            if splits is not None and len(splits) > 0:
                filename = "splits.csv"
                if save_data(pd.DataFrame(splits), filename, output_dir, logger):
                    successful_files += 1
                    generated_files.append(os.path.join(output_dir, filename))
        except Exception as e:
            logger.warning(f"Failed to fetch splits: {str(e)}")

        # Create a summary report
        summary = f"""
        Stock Data Scraping Summary Report
        ===============================
        Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
        Company: {ticker_name}
        Ticker: {ticker_symbol}
        Files Generated: {successful_files}

        Data Collected:
        - Company Information
        - Historical Market Data
        - Financial Statements (Annual & Quarterly)
        - Major Shareholders
        - Analyst Recommendations
        - ESG Data (if available)
        - News Articles
        - Dividends and Splits

        All data has been saved to the '{output_dir}' directory.
        """

        filename = "summary_report.txt"
        save_data(summary, filename, output_dir, logger)
        summary_report_path = os.path.join(output_dir, filename)
        generated_files.append(summary_report_path)
        logger.info("Created summary report")

        return successful_files > 0, generated_files, summary_report_path

    except Exception as e:
        logger.error(f"Error occurred while scraping data: {str(e)}")
        return False, [], None


# Function to generate tickers for Indian exchanges
def get_indian_tickers(base_ticker):
    # Only use Indian exchanges (NS)
    extensions = [".NS"]
    tickers = [f"{base_ticker}{ext}" for ext in extensions]
    return tickers


# Function to download CSV
def get_csv_download_link(df, filename):
    csv = df.to_csv(index=True)
    b64 = base64.b64encode(csv.encode()).decode()
    href = f'<a href="data:file/csv;base64,{b64}" download="{filename}">Download CSV</a>'
    return href


# Function to run the scraper in a separate thread
def run_scraper(ticker_name, retry_count, delay):
    with st.spinner(f"Fetching data for {ticker_name}. This may take a few minutes..."):
        # Set up logging
        logger, log_handler = setup_logging(ticker_name)

        base_ticker = ticker_name.replace(" ", "")
        tickers = get_indian_tickers(base_ticker)

        results = {}
        successful_tickers = []
        generated_files = []
        summary_report_path = None

        # Process all tickers
        for ticker in tickers:
            logger.info(f"Attempting to scrape data for ticker: {ticker}")
            result, files, summary_path = get_stock_data(ticker, ticker_name, retry_count=retry_count, delay=delay,
                                                         logger=logger)
            results[ticker] = result

            if result:
                logger.info(f"Successfully scraped data using ticker: {ticker}")
                successful_tickers.append(ticker)
                generated_files.extend(files)
                if summary_path:
                    summary_report_path = summary_path
            else:
                logger.warning(f"Failed to scrape complete data using ticker: {ticker}")

            # Add a delay between requests to avoid rate limiting
            if ticker != tickers[-1]:
                time.sleep(delay)

        # Get log output
        log_stream = log_handler.stream.getvalue()

        # Summary of all attempts
        success_count = len(successful_tickers)
        logger.info(f"Successfully scraped data for {success_count} out of {len(tickers)} tickers")

        if success_count == 0:
            logger.error("Failed to scrape data using any of the provided tickers")
            return False, [], None, log_stream
        else:
            logger.info(f"Data scraping successful for: {', '.join(successful_tickers)}")
            return True, generated_files, summary_report_path, log_stream


# Main Streamlit app
def main():
    st.title("📊 Stock Data Scraper")
    st.markdown("Enter a company name to fetch and analyze stock data from Indian exchanges.")

    # User input
    with st.form("stock_input_form"):
        col1, col2 = st.columns([3, 1])
        with col1:
            ticker_name = st.text_input("Enter company's ticker name (e.g., TCS, INFY, RELIANCE):")
        with col2:
            retry_count = st.number_input("Retry count:", min_value=1, max_value=5, value=3)
            delay = st.number_input("Delay between requests (seconds):", min_value=1, max_value=10, value=5)

        submitted = st.form_submit_button("Fetch Data")

    # Check if form is submitted
    if submitted and ticker_name:
        # Store the results in session state
        if 'processing' not in st.session_state:
            st.session_state.processing = True
            st.session_state.completed = False

            # Run the scraper in a separate thread
            success, generated_files, summary_report_path, log_output = run_scraper(ticker_name, retry_count, delay)

            # Update session state with results
            st.session_state.processing = False
            st.session_state.completed = True
            st.session_state.success = success
            st.session_state.generated_files = generated_files
            st.session_state.summary_report_path = summary_report_path
            st.session_state.log_output = log_output

            # Force a rerun to display the results
            st.rerun()

    # Display results if processing is complete
    if 'completed' in st.session_state and st.session_state.completed:
        if st.session_state.success:
            st.success(f"Successfully scraped data for {ticker_name}")

            # Display summary report first if available
            if 'summary_report_path' in st.session_state and st.session_state.summary_report_path:
                st.subheader("📑 Summary Report")
                try:
                    with open(st.session_state.summary_report_path, 'r') as f:
                        summary_content = f.read()

                    st.text_area("", summary_content, height=300)

                except Exception as e:
                    st.error(f"Error reading summary report: {str(e)}")

            # Display generated files
            if st.session_state.generated_files:
                st.subheader("Generated Files")

                # Group files by directory
                files_by_dir = {}
                for file_path in st.session_state.generated_files:
                    dir_name = os.path.dirname(file_path)
                    if dir_name not in files_by_dir:
                        files_by_dir[dir_name] = []
                    files_by_dir[dir_name].append(file_path)

                # Display files by directory as expandable sections
                for dir_name, files in files_by_dir.items():
                    with st.expander(f"Directory: {os.path.basename(dir_name)}"):
                        for file_path in sorted(files):
                            file_name = os.path.basename(file_path)
                            # Skip summary report in the file listing as it's shown above
                            if file_name == "summary_report.txt":
                                continue

                            col1, col2 = st.columns([3, 1])

                            with col1:
                                if file_name.endswith('.csv'):
                                    if st.button(f"📄 {file_name}", key=f"view_{file_path}"):
                                        st.session_state.selected_file = file_path
                                else:
                                    if st.button(f"📝 {file_name}", key=f"view_{file_path}"):
                                        st.session_state.selected_file = file_path

                            with col2:
                                if file_name.endswith('.csv'):
                                    try:
                                        df = pd.read_csv(file_path)
                                        st.markdown(get_csv_download_link(df, file_name), unsafe_allow_html=True)
                                    except Exception as e:
                                        st.error(f"Error reading CSV: {str(e)}")

                # Display selected file content
                if 'selected_file' in st.session_state:
                    file_path = st.session_state.selected_file
                    file_name = os.path.basename(file_path)

                    st.subheader(f"Viewing: {file_name}")

                    if file_name.endswith('.csv'):
                        try:
                            df = pd.read_csv(file_path)
                            st.dataframe(df, use_container_width=True)
                        except Exception as e:
                            st.error(f"Error reading CSV: {str(e)}")
                    else:
                        try:
                            with open(file_path, 'r') as f:
                                content = f.read()
                            st.text_area("File Content", content, height=300)
                        except Exception as e:
                            st.error(f"Error reading file: {str(e)}")

            # Display log output
            with st.expander("View Log Output"):
                st.text_area("Log", st.session_state.log_output, height=300)
        else:
            st.error(f"Failed to scrape data for {ticker_name}")

            # Display log output
            with st.expander("View Log Output"):
                st.text_area("Log", st.session_state.log_output, height=300)

        # Reset button
        if st.button("Start New Search"):
            for key in ['processing', 'completed', 'success', 'generated_files', 'summary_report_path', 'log_output',
                        'selected_file']:
                if key in st.session_state:
                    del st.session_state[key]
            st.rerun()  # Updated from experimental_rerun()


if __name__ == "__main__":
    main()