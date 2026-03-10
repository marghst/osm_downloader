# OSM to PostGIS: Downloader de dados do OSM

Este projeto consiste numa ferramenta automatizada para extrair, processar e carregar dados do **OpenStreetMap (OSM)** diretamente para uma base de dados **PostgreSQL/PostGIS**.

O script utiliza os limites oficiais da **CAOP (DGT)** para garantir que os dados extraídos (estradas, pontos de interesse, etc.) são recortados exatamente pelos limites administrativos oficiais.

## Funcionalidades

* Os dados são processados na memória RAM e enviados diretamente para o SQL, sem necessidade de criar ficheiros temporários no disco. No entanto, tem a opção de guardar no disco e/ou na base de dados.

* A área de interesse é definida através do código Dicofre/Dtmnfr, sendo possível selecionar dados de todo um **distrito**, de um só **município** ou de uma só **freguesia**.

* Filtra POIs irrelevantes (sem 'name', 'brand' ou 'operator').

* O script cria automaticamente o Schema e ativa a extensão PostGIS na base de dados.

## Estrutura do Projeto

* `main.py`: Script principal de execução
* `config.py.example`: Definições de filtros OSM e colunas a extrair (deve ser alterado)
* `database_config.py.example`: Credenciais de base de dados (deve ser alterado)
* `data/`: Pasta onde deve ser colocado o ficheiro da CAOP (`.gpkg`)
* `requirements.txt`: Lista de dependências

## Instalação

1.  **Preparar o Ambiente (Windows):**
    ```bash
    python -m venv venv
    .\venv\Scripts\activate
    pip install -r requirements.txt
    ```

2.  **Configurar Credenciais:**
    * Crie uma base de dados no **pgAdmin**.
    * Renomeie o ficheiro `database_config.py.example` para `database_config.py`, e `config.py.example` para `config.py.`.
    * Edite os ficheiros com as suas credenciais e o código da área de interesse.
    

3.  **Executar:**
    ```bash
    python main.py
    ```

## Requisitos de dados
O script espera encontrar o ficheiro da CAOP na pasta `data/`. 
Exemplo: `data/Continente_CAOP2025.gpkg`.