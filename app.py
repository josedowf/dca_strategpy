from flask import Flask, render_template, request, jsonify
from main import dca_script, defaults
from utils import today_formatted, open_csv_file
import datetime

app = Flask(__name__)


@app.route('/')
def index():
    return render_template('index.html', defaults=defaults)


@app.route('/get_data', methods=['POST'])
def get_data():
    ticker = request.form['ticker']

    start_range = request.form['start_range']
    start_date = datetime.datetime.strptime(start_range, '%Y-%m-%d')
    start_date_str = datetime.datetime.strftime(start_date, '%Y-%m-%d')

    end_date = today_formatted()

    dca_periodicity = request.form['dca_periodicity']

    dc_amount = int(request.form['dc_amount'])

    dca_script(start_date_str, dca_periodicity, dc_amount, end_date, ticker)

    return 'Script executed successfully!'


if __name__ == '__main__':
    app.run(debug=True)
