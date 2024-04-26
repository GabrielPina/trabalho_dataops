# de04_dataops

Projeto disciplina de DataOps
- Ciclo de vida de projeto de dados
- Pipeline de dados
- Ferramentas básicas em dataops

## **Projeto**
Este projeto tem como objetivo criar um pipeline de ingestão de dados da api: https://randomuser fazer um tratamento simples (saneamento) de acordo com um metadado e salvar estes dados em uma tabela no mysql. Tanto o pipeline quanto o banco devem estar em um docker.



![alt text](imgs/projeto_final.png)

```
dataops04
│   README.md
│   .gitignore 
│   .github/workflows
|   README.md
|   docker-compose.yml
└───imgs
|
└───python
│   |   Dockerfile
|   └────scripts
│   │   │   ingestion.py
│   │   │   config.py
│   │   │   utils.py
│   │   │   metadado.xlsx
│   └────data
│   │   │   └─── raw
|   │   |   └─── work
|
└───mysql
│   |   Dockerfile
│   └────db
|   |   |   CreateDtabase.sql
```

