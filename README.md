Stock Investment Program
This is a Python program that utilizes Polygon's API to extract real-time stock data, stores it in a PostgreSQL database, and runs a dollar-costing average investment strategy scenario based on user-defined parameters.

Features:
Extracts real-time stock data from Polygon's API.
Stores data in a PostgreSQL database.
Runs a dollar-costing average investment strategy based on user-defined parameters.
Flask-based HTML interface for user input.
Dependencies

This program requires the following dependencies:
Python 3.6 or higher
PostgreSQL
Flask
SQLAlchemy
Polygon API key

Installation and Usage:
Clone the repository to your local machine.
Install the dependencies using pip: pip install -r requirements.txt
Create a PostgreSQL database and update the database connection details in creds.json.
Set your Polygon API key in creds.json
Run the program using python app.py.
Access the HTML interface at http://localhost:5000 and input the desired company ticker, start range, dollar costing amount, and frequency of dollar costing.
The program will execute the dollar-cost averaging investment strategy using the user-defined parameters and display the results on the HTML interface.

