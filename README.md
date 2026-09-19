# Cidade Inteligente: ETL de dados climáticos

Projeto da disciplina de Engenharia de Dados e Big Data que implementa, em Python e com orientação a objetos, o fluxo obrigatório:

```text
API Open-Meteo
      ↓
MongoDB Atlas (dados brutos)
      ↓
Transformação com pandas
      ↓
NeonDB/PostgreSQL (dados tratados)
```

Os dados da API são gravados primeiro no Atlas. O pipeline recupera exatamente o documento recém-gravado, transforma sua série horária e só então carrega o resultado no NeonDB.

## Organização

- `src/extract.py`: consulta a Open-Meteo e recupera dados brutos do Atlas;
- `src/transform.py`: converte o JSON bruto em um `DataFrame` limpo e tipado;
- `src/load.py`: carrega dados brutos no Atlas e dados tratados no NeonDB;
- `run_etl.py`: orquestra o fluxo completo na ordem exigida.

As cidades cadastradas são `recife`, `sao_paulo`, `rio_de_janeiro` e `brasilia`. O exemplo executado usa Recife, de 1 a 7 de janeiro de 2024, totalizando 168 medições horárias.

## Requisitos

- Python 3.10 ou superior;
- cluster e usuário configurados no MongoDB Atlas;
- projeto e banco PostgreSQL configurados no Neon;
- acesso à internet.

## Instalação

No PowerShell, na raiz do projeto:

```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

Se você já usa o ambiente virtual `env`, basta ativá-lo e executar novamente o comando de instalação para adicionar o driver PostgreSQL.

## Variáveis de ambiente

Crie ou complete o arquivo `.env` na raiz:

```env
MONGODB_URI=mongodb+srv://USUARIO:SENHA@SEU_CLUSTER.mongodb.net/?retryWrites=true&w=majority
NEON_DATABASE_URL=postgresql://USUARIO:SENHA@SEU_ENDPOINT.neon.tech/neondb?sslmode=require&channel_binding=require
```

### MongoDB Atlas

No Atlas, use **Connect → Drivers → Python**, copie a URI e substitua a senha. O IP da máquina deve estar liberado em **Network Access**.

### NeonDB

1. Crie uma conta e um projeto em <https://console.neon.tech/>.
2. No painel do projeto, clique em **Connect**.
3. Selecione a branch, o banco e o usuário desejados.
4. Escolha **Python** ou apenas copie a **Connection string**.
5. Cole a URI completa em `NEON_DATABASE_URL`.

Não versionar o `.env`: ele contém as senhas dos dois bancos. Arquivos terminados em `.env` também estão ignorados pelo Git.

## Execução

```powershell
python run_etl.py
```

O console demonstra explicitamente as quatro etapas:

```text
1/4 - Extraindo dados da API...
2/4 - Armazenando dados brutos no MongoDB Atlas...
3/4 - Recuperando do Atlas e transformando...
4/4 - Armazenando dados transformados no NeonDB...
```

## Bancos gerados

No MongoDB Atlas, cada execução adiciona o JSON bruto em:

- banco: `cidade_inteligente`;
- coleção: `clima_recife`.

No NeonDB, o método cria automaticamente a tabela `historico_clima` com:

| Coluna | Tipo | Descrição |
| --- | --- | --- |
| `id` | `BIGSERIAL` | chave primária |
| `cidade` | `VARCHAR(100)` | cidade consultada |
| `data_hora` | `TIMESTAMP` | data e hora da medição |
| `temperatura` | `DOUBLE PRECISION` | temperatura em °C |
| `umidade` | `DOUBLE PRECISION` | umidade relativa em % |
| `chuva` | `DOUBLE PRECISION` | precipitação em mm |

A combinação `cidade + data_hora` é única. Se o pipeline for executado novamente para o mesmo período, os valores são atualizados por `upsert`, sem duplicar as medições no Neon.

Para conferir no **SQL Editor** do Neon:

```sql
SELECT COUNT(*) FROM historico_clima;

SELECT *
FROM historico_clima
ORDER BY data_hora
LIMIT 10;
```

## Tratamento dos dados

`Transform.transform_clima()` renomeia as colunas da API para `data_hora`, `temperatura`, `umidade` e `chuva`; converte data/hora e números para seus tipos apropriados; e substitui chuva ausente por `0.0`.

## Critérios atendidos

- API pública relacionada a Smart Cities;
- código Python organizado nas classes `Extract`, `Transform` e `Load`;
- dados brutos persistidos no banco NoSQL MongoDB Atlas;
- leitura do documento diretamente do Atlas antes da transformação;
- transformação com pandas;
- dados tratados persistidos no banco relacional PostgreSQL do NeonDB;
- conexões protegidas por variáveis de ambiente;
- carga relacional repetível sem duplicação.

## Integrantes

- [GabrielHen-dev](https://github.com/GabrielHen-dev)
- [dgcavalcante](https://github.com/dgcavalcante)
- [lucaschavessf](https://github.com/lucaschavessf)
- [allyssonifx](https://github.com/allyssonifx)
- [Fernando2732](https://github.com/Fernando2732)
- [MatheusAS1](https://github.com/MatheusAS1)
- [Carlos Henrique](https://github.com/Carlos-HenriqueDev)
- [Allana Gadêlha](https://github.com/Allana-Gadelha)

## Referências

- [Open-Meteo](https://open-meteo.com/)
- [MongoDB PyMongo](https://www.mongodb.com/docs/languages/python/pymongo-driver/current/)
- [Neon Docs](https://neon.com/docs/)
- [Psycopg](https://www.psycopg.org/psycopg3/docs/)
- [Pandas](https://pandas.pydata.org/docs/)
