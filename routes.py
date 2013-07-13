from flask import Flask
#import analyze_data


app = Flask("Charlie")

@app.route("/")
def hello():
	return "Hi, Adam!"

if __name__ == "__main__":
	app.run()