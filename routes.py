from flask import Flask
#import analyze_data


app = Flask("Charlie")

@app.route("/")
def hello():
	return "Charlie's gonna win!!"

if __name__ == "__main__":
	app.run()