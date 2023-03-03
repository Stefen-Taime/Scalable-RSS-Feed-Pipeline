from pymongo import MongoClient
from bson.objectid import ObjectId
from flask import Flask, request, jsonify, render_template

client = MongoClient('mongodb://debezium:dbz@localhost:27017/?authSource=admin')
db = client['demo']
collection = db['rss_feeds_collection']


app = Flask(__name__, template_folder='/path/template')

# get all news articles
@app.route('/news', methods=['GET'])
def get_all_news():
    cursor = collection.find({}, {"_id": 0})
    news = []
    for item in cursor:
        news.append({'title': item['title'], 'summary': item['summary'], 'link': item['link'], 'language': item['language'], 'id': item['id']})
    return jsonify({'news': news})

# get a news article by id
@app.route('/news/<id>', methods=['GET'])
def get_news_by_id(id):
    item = collection.find_one({'id': id})
    if item:
        return jsonify({'_id': str(item['_id']), 'title': item['title'], 'summary': item['summary'], 'link': item['link'], 'language': item['language']})
    else:
        return jsonify({'error': 'News article not found'})

# update a news article by id
@app.route('/news/<id>', methods=['PUT'])
def update_news_by_id(id):
    item = collection.find_one({'id': id})
    if item:
        data = request.get_json()
        collection.update_one({'id': id}, {'$set': data})
        return jsonify({'message': 'News article updated successfully'})
    else:
        return jsonify({'error': 'News article not found'})

# delete a news article by id
@app.route('/news/<id>', methods=['DELETE'])
def delete_news_by_id(id):
    item = collection.find_one({'id': id})
    if item:
        collection.delete_one({'id': id})
        return jsonify({'message': 'News article deleted successfully'})
    else:
        return jsonify({'error': 'News article not found'})


# render a web page with news articles
@app.route('/', methods=['GET'])
def news_page():
    page = request.args.get('page', 1, type=int)
    language = request.args.get('language')

    # build query for language filtering
    query = {} if not language else {'language': language}

    # retrieve total count and paginated news articles
    count = collection.count_documents(query)
    cursor = collection.find(query, {"_id": 0}).skip((page-1)*5).limit(8)
    news = []
    for item in cursor:
        news.append({'title': item['title'], 'summary': item['summary'], 'link': item['link'], 'language': item['language'], 'id': item['id']})

    # calculate number of pages for pagination
    num_pages = count // 8 + (1 if count % 8 > 0 else 0)

    return render_template('index.html', news=news, page=page, language=language, num_pages=num_pages)

if __name__ == '__main__':
    app.run(debug=True)
