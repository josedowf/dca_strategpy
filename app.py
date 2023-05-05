from flask import Flask, render_template, request
from main import dca_script, dca_script_multiple_companies
from utils import today_formatted
import datetime

app = Flask(__name__, static_folder='static')
tickers = []


@app.route('/', methods=['GET', 'POST'])
def index():
    return render_template('index.html')


@app.route('/single_company_DCA', methods=['GET', 'POST'])
def single_companies_dca():
    if request.method == 'POST':
        ticker = request.form['ticker']

        start_range = request.form['start_range']
        start_date = datetime.datetime.strptime(start_range, '%Y-%m-%d')
        start_date_str = datetime.datetime.strftime(start_date, '%Y-%m-%d')

        end_date = today_formatted()

        dca_periodicity = request.form['dca_periodicity']

        dc_amount = int(request.form['dc_amount'])

        dca_script(start_date_str, dca_periodicity, dc_amount, end_date, ticker)

        return render_template('index.html'), 200, {}

    return render_template('single_company_DCA.html')


@app.route('/multiple_company_DCA', methods=['GET', 'POST'])
def multiple_companies_dca():
    if request.method == 'POST':
        if 'ticker_submit' in request.form:
            ticker = request.form['ticker']
            ticker = ticker.upper()
            tickers.append(ticker)
            print(tickers)

        if 'form_submit' in request.form:
            start_range = request.form['start_range']
            start_date = datetime.datetime.strptime(start_range, '%Y-%m-%d')
            start_date_str = datetime.datetime.strftime(start_date, '%Y-%m-%d')

            end_date = today_formatted()

            dca_periodicity = request.form['dca_periodicity']

            dc_amount = int(request.form['dc_amount'])

            dca_script_multiple_companies(start_date_str, dca_periodicity, dc_amount, end_date, tickers)

            return 'Script executed successfully!', 200, {}

    return render_template('multiple_company_DCA.html')


if __name__ == '__main__':
    app.run(debug=True)
