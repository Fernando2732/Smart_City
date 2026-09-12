# Cidade Inteligente: ETL de dados climáticos

Projeto desenvolvido para a disciplina de Engenharia de Dados e Big Data com o objetivo de aplicar um pipeline **ETL (Extract, Transform, Load)** a uma fonte pública relacionada a Smart Cities.

A fonte escolhida foi a [Open-Meteo](https://open-meteo.com/), uma API pública de dados meteorológicos que não exige chave de acesso. O pipeline consulta dados climáticos históricos por cidade, grava o dado bruto no MongoDB, organiza os registros horários com pandas e grava o resultado transformado em SQLite.

## Objetivo do projeto

Reaproveitar a estrutura do projeto original da disciplina para trabalhar com uma nova fonte de dados de interesse urbano. Os dados de clima podem apoiar análises de temperatura, umidade e chuva em uma cidade ao longo do tempo.

## Arquitetura

O projeto está dividido em três responsabilidades:

- **Extract** (`src/extract.py`): consulta a API Open-Meteo e retorna o JSON bruto. A classe mantém um dicionário de cidades e suas coordenadas, valida a cidade informada e usa `requests.get()` com `raise_for_status()`.
- **Transform** (`src/transform.py`): extrai a série horária do JSON, cria um `DataFrame`, renomeia as colunas, converte os tipos e substitui valores ausentes de chuva por `0.0`.
- **Load** (`src/load.py`): disponibiliza a persistência dos dados brutos em arquivo JSON e MongoDB, além da gravação do `DataFrame` transformado em uma tabela SQLite.

Fluxo resumido:

```text
Open-Meteo
    |
    v
Extract.clima()
    |
    v
JSON bruto -> JSON / MongoDB
    |
    v
Transform.transform_clima()
    |
    v
DataFrame -> SQLite

```

## Fonte de dados

O projeto utiliza o endpoint histórico da Open-Meteo:

`https://archive-api.open-meteo.com/v1/archive`

As variáveis horárias consultadas são:

- `temperature_2m`: temperatura a 2 metros;
- `relative_humidity_2m`: umidade relativa;
- `rain`: precipitação em milímetros.

As cidades disponíveis em `Extract.CIDADES` são:

- `recife`;
- `sao_paulo`;
- `rio_de_janeiro`;
- `brasilia`.

Também é possível informar os nomes com espaços e letras maiúsculas, pois o método normaliza o valor recebido.

## Estrutura do projeto

```text
.
├── jsons/
│   └── clima_recife.json       # exemplo de dado bruto
├── notebooks/
│   └── teste.ipynb             # exploração do DataFrame
├── src/
│   ├── extract.py              # extração na API e leitura do MongoDB
│   ├── load.py                 # cargas em JSON, MongoDB e SQLite
│   └── transform.py            # transformação com pandas
├── requirements.txt
└── run_etl.py                  # execução do pipeline
```

## Requisitos

- Python 3.10 ou superior;
- acesso à internet para consultar a Open-Meteo;
- uma instância MongoDB acessível;
- `pip` para instalar as dependências.

As bibliotecas utilizadas estão em `requirements.txt`:

- `requests`;
- `pandas`;
- `pymongo`;
- `python-dotenv`.

## Instalação

No terminal, na raiz do projeto:

```bash
python -m venv .venv
```

Ative o ambiente virtual:

```powershell
.venv\Scripts\Activate.ps1
```

No Linux ou macOS:

```bash
source .venv/bin/activate
```

Instale as dependências:

```bash
pip install -r requirements.txt
```

## Configuração do MongoDB

Crie um arquivo `.env` na raiz do projeto com a variável `MONGODB_URI`:

```env
MONGODB_URI=mongodb+srv://usuario:senha@cluster.exemplo.mongodb.net/?retryWrites=true&w=majority
```

Não versionar o arquivo `.env`, pois ele pode conter credenciais de acesso.

## Execução

Execute:

```bash
python run_etl.py
```

O script atualmente consulta:

- cidade: `recife`;
- início: `2024-01-01`;
- fim: `2024-01-07`;
- fuso horário: `America/Sao_Paulo`.

Ao final, o dado bruto é inserido no banco `cidade_inteligente`, na coleção `clima_recife`, o `DataFrame` é gravado no banco `clima.db`, na tabela `historico_clima`, e uma cópia do dado bruto é salva em `jsons/clima_recife.json`.

## Dados transformados

O método `Transform.transform_clima()` produz as seguintes colunas:

| Coluna | Descrição |
| --- | --- |
| `data_hora` | data e hora do registro |
| `temperatura` | temperatura em graus Celsius |
| `umidade` | umidade relativa em porcentagem |
| `chuva` | precipitação em milímetros |

`data_hora` é convertida para o tipo de data/hora do pandas. As medidas numéricas são convertidas para `float`, e valores ausentes de chuva são preenchidos com `0.0`.

## Persistência

O projeto possui os seguintes métodos de carga:

- `Load.load_json()`: salva uma cópia do dado bruto em `jsons/`;
- `Load.load_mongo()`: insere o dado bruto em uma coleção MongoDB definida pelos parâmetros `db_name` e `collection_name`;
- `Load.load_sqlite()`: salva o `DataFrame` em uma tabela SQLite, substituindo a tabela existente quando ela já existe.

O fluxo executado usa `load_json()`, `load_mongo()` e `load_sqlite()`. A chamada utilizada para a coleção climática é:

```python
loader.load_mongo(
    dados_brutos,
    db_name="cidade_inteligente",
    collection_name=f"clima_{cidade}",
)
```

Depois dessa chamada, é recomendado executar `loader.close()` quando todas as cargas terminarem.

## Critérios atendidos

- consumo de uma API pública real de Smart City;
- método de extração com parâmetros tipados;
- validação das cidades disponíveis por meio de dicionário de classe;
- URL da API centralizada como atributo da classe `Extract`;
- transformação do JSON bruto em `pandas.DataFrame`;
- persistência em SQLite;
- persistência do bruto em MongoDB usando `MONGODB_URI`;
- separação das responsabilidades em `Extract`, `Transform` e `Load`.

## Possíveis análises

Com os dados carregados, podem ser feitas análises como:

- variação horária da temperatura;
- comparação de umidade entre cidades;
- identificação de períodos com chuva;
- comparação de condições climáticas entre diferentes intervalos de datas;
- criação de gráficos e indicadores para apoiar decisões de uma cidade inteligente.

## Integrantes

- [Gabriel Henrique](https://github.com/GabrielHen-dev)
- [Diogo Cavalcante](https://github.com/dgcavalcante)
- [Lucas Chaves](https://github.com/lucaschavessf)
- [Allysson](https://github.com/allyssonifx)
- [Fernando Marinho](https://github.com/Fernando2732)
- [Matheus Andrade](https://github.com/MatheusAS1)
- [Carlos Henrique](https://github.com/Carlos-HenriqueDev)
- [Allana Gadêlha](https://github.com/Allana-Gadelha)

## Referências

- [Open-Meteo](https://open-meteo.com/)
- [Open-Meteo Historical Weather API](https://open-meteo.com/en/docs/historical-weather-api)
- [Pandas](https://pandas.pydata.org/docs/)
- [PyMongo](https://pymongo.readthedocs.io/)
