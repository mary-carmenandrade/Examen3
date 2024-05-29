from flask import Flask, render_template, request
import pandas as pd
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.metrics.pairwise import cosine_similarity
from flask_redis import FlaskRedis

app = Flask(__name__)
app.config['REDIS_URL'] = 'redis://my-redis-container:6379/0'
redis_client = FlaskRedis(app)

# Load the CSV and print columns to ensure 'title' exists
df = pd.read_csv('movies2.csv')
print(f"Initial columns: {df.columns}")

# Clean the 'genres' column and ensure 'title' column is lowercased
df['genres'] = df['genres'].apply(lambda genres: str(genres).lower().replace('|', ' '))
df['title'] = df['title'].str.lower()

# Drop 'movieId' and combine other columns into a 'data' column
df2 = df.drop(['movieId'], axis=1)
df2['data'] = df2[df2.columns[1:]].apply(lambda x: ' '.join(x.dropna().astype(str)), axis=1)
print(f"Columns after dropping and combining: {df2.columns}")

# Vectorize the 'data' column and compute similarities
vectorizer = CountVectorizer()
vectorized = vectorizer.fit_transform(df2['data'])
similarities = cosine_similarity(vectorized)

# Creating a DataFrame for similarities, reset index, and print columns
df_similarity = pd.DataFrame(similarities, columns=df['title'], index=df['title']).reset_index()
print(f"Columns in similarity DataFrame: {df_similarity.columns}")

@app.route('/', methods=['GET', 'POST'])
def index():
    if request.method == 'POST':
        input_genre = request.form['input_genre'].lower()
        recommendations = get_recommendations(input_genre)
        return render_template('index.html', movies=recommendations, message="Recomendaciones generadas con éxito!")
    return render_template('index.html')

def get_recommendations(input_genre):
    stored_recommendations = redis_client.get(input_genre)
    if stored_recommendations:
        return [movie.split(',') for movie in stored_recommendations.decode('utf-8').split(',')]
    else:
        if input_genre not in df_similarity['title'].values:
            similar_genres = find_similar_genres(input_genre)
            if similar_genres:
                redis_client.set(input_genre, ','.join(similar_genres))
                return [movie.split(',') for movie in similar_genres]
        else:
            recommendations = pd.DataFrame(df_similarity.nlargest(11, input_genre)['title'])
            recommendations = recommendations[recommendations['title'] != input_genre]
            recommended_movies = recommendations['title'].values.tolist()
            redis_client.set(input_genre, ','.join(recommended_movies))
            return [movie.split(',') for movie in recommended_movies]

def find_similar_genres(input_genre):
    input_genre_vector = vectorizer.transform([input_genre])
    similarities = cosine_similarity(input_genre_vector, vectorized)
    similar_genres_indices = similarities.argsort()[0][-11:-1]
    similar_genres = df_similarity['title'].iloc[similar_genres_indices].values.tolist()
    return similar_genres

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=80, debug=True, threaded=True)
