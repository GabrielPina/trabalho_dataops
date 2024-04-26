USE db;
CREATE TABLE cadastro (
    id integer not null auto_increment,
    sexo varchar(200),
    titulo varchar(200),
    nome varchar(200),
    sobrenome varchar(200),
    cidade varchar(200),
    estado varchar(200),
    pais varchar(200),
    email varchar(200),
    data_nascimento date,
    load_date datetime not null,
    KEY (id)    
);
SET character_set_client = utf8;
SET character_set_connection = utf8;
SET character_set_results = utf8;
SET collation_connection = utf8_general_ci;
INSERT INTO cadastro (id, sexo, titulo, nome, sobrenome, cidade, estado, pais, email, data_nascimento, load_date) 
VALUES (1, "female", "Miss", "Victoria", "Lambert", "Gloucester", "Rutland", "United Kingdom", "victoria.lambert@example.com", "1965-06-20", "2024-03-16");