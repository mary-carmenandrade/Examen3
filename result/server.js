const express = require('express');
const { Pool } = require('pg');
const app = express();

const pool = new Pool({
    host: 'mi_postgres_container',
    port: 5432,
    database: 'recomendaciones_peliculas',
    user: 'postgres',
    password: 'password'
});

app.set('view engine', 'ejs');

app.get('/movies', async (req, res) => {
    try {
        const result = await pool.query("SELECT * FROM movies");
        const movies = result.rows.map(row => [row.movie_name, row.movie_list]);
        res.render('index', { movies });
    } catch(error) {
        console.error(error);
        res.status(500).send('Error obteniendo las películas');
    }
});

app.listen(3000, () => {
    console.log('Servidor Node.js escuchando en el puerto 3000');
});
