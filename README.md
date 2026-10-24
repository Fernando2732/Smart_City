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
      ↓
Dashboard Streamlit
```

Os dados da API são gravados primeiro no Atlas. O pipeline recupera exatamente o documento recém-gravado, transforma sua série horária e só então carrega o resultado no NeonDB.

## Organização

- `src/config.py`: carrega o `.env` e centraliza as conexões;
- `src/extract.py`: consulta a Open-Meteo e recupera dados brutos do Atlas;
- `src/transform.py`: converte o JSON bruto em um `DataFrame` limpo e tipado;
- `src/load.py`: carrega dados brutos no Atlas e dados tratados no NeonDB;
- `orchestration_prefect.py`: define as quatro tasks e o flow sequencial do Prefect;
- `run_etl.py`: ponto de entrada que chama o flow;
- `dashboard/app.py`: lê os dados tratados do Neon e apresenta filtros, gráfico e indicador.

As cidades cadastradas são `recife`, `sao_paulo`, `rio_de_janeiro` e `brasilia`. O exemplo executado usa Recife, de 1 a 7 de janeiro de 2024, totalizando 168 medições horárias.

## Requisitos

- Python 3.10 ou superior;
- cluster e usuário configurados no MongoDB Atlas;
- projeto e banco PostgreSQL configurados no Neon;
- acesso à internet.

## Instalação

Execute os comandos abaixo na raiz do projeto.

Para criar e ativar o ambiente (somente se ainda não tiver um):

```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
python -m pip install -r requirements.txt
```

Se já tiver `.venv`, pule a criação e apenas ative o ambiente. Se usa `env`, ative `env\Scripts\Activate.ps1`. Com o ambiente ativo, execute `python -m pip install -r requirements.txt` para instalar também o Streamlit.

## Variáveis de ambiente

Crie ou complete o arquivo `.env` na raiz de `Smart_City`, usando `.env.example` como referência.

```env
MONGODB_URI=mongodb+srv://USUARIO:SENHA@SEU_CLUSTER.mongodb.net/?retryWrites=true&w=majority
NEON_DATABASE_URL=postgresql://USUARIO:SENHA@SEU_ENDPOINT.neon.tech/neondb?sslmode=require&channel_binding=require
```

`DATABASE_URL` também é aceita quando `NEON_DATABASE_URL` não está definida. A configuração é carregada uma única vez por processo e compartilhada pelo pipeline e pelo dashboard.

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

Com o ambiente ativo e as credenciais configuradas, rode o pipeline:

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

O flow `ETL Clima Open-Meteo` chama as tasks diretamente, uma por vez:

- `extract`: consulta a API com `Extract.clima()`;
- `load_data`: grava o JSON bruto com `Load.load_mongo()` e retorna seu `ObjectId`;
- `transform`: recupera esse documento do Atlas e chama `Transform.transform_clima()`;
- `load_neon`: envia o `DataFrame` para `Load.load_neon()`, mantendo a tabela `historico_clima`.

Cada task só inicia após a anterior terminar com sucesso. Falhas são propagadas ao flow e impedem as próximas etapas. As conexões MongoDB de cada task são fechadas em `finally`; a conexão PostgreSQL continua sendo gerenciada pelo método de carga existente.

A extração e a carga no Neon têm até três retries, com intervalo de dez segundos. A carga no Neon usa o `upsert` existente. A escrita no MongoDB e o flow completo têm retries desativados, pois repetir `insert_one` criaria outro documento bruto. Novas execuções manuais continuam adicionando um documento no Atlas, conforme o comportamento original. A persistência de resultados das tasks está desativada para que cada execução processe seus próprios dados.

O Prefect registra estados e logs das tasks e do flow. A execução local não exige agendamentos, deployments, workers ou Prefect Cloud.

Para validar localmente, sem consultar a API nem escrever nos bancos:

```powershell
python -m unittest discover -s tests -v
```

Os testes de orquestração usam o Prefect real com API, MongoDB e Neon substituídos por mocks. A validação de ponta a ponta depende das credenciais no `.env` e de acesso aos serviços.

## Bancos gerados

No MongoDB Atlas, cada execução adiciona o JSON bruto em:

- banco: `cidade_inteligente`;
- coleção: `clima_recife`.

O documento recebe `data_extracao` em UTC. Novas execuções acrescentam documentos, sem apagar o histórico. Documentos antigos continuam preservados, mesmo que ainda não tenham esse campo.

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

`Transform.transform_clima()` renomeia as colunas da API para `data_hora`, `temperatura`, `umidade` e `chuva` e converte data/hora e números para seus tipos apropriados. Todas as medições de `hourly` são mantidas.

Valores `null` no JSON são recebidos como `None` no Python, preservados como ausências no pandas e enviados como `NULL` ao Neon. A chuva ausente não é mais substituída por `0.0`; o zero informado pela fonte continua sendo zero. A mesma preservação vale para temperatura e umidade. A ocorrência de ausências na carga real deve ser conferida nas consultas abaixo; os testes incluem esse caso explicitamente.

Para corrigir possíveis zeros artificiais de uma execução anterior, execute novamente `python run_etl.py` para o período de exemplo. O `upsert` atualiza essas medições no Neon. Outros períodos já carregados precisam ser reprocessados com os respectivos parâmetros do flow; não é possível distinguir um zero artificial de um zero real apenas pela tabela final.

## Adequações da Aula 08

- **Configuração:** as leituras de credenciais e o `load_dotenv()` ficam somente em `src/config.py`.
- **Dados completos:** o Transform recebe um documento com listas de medições horárias e converte todas elas, sem selecionar apenas o primeiro resultado. O flow confere a quantidade de horários no bronze e de linhas transformadas. Metadados da resposta, como coordenadas e unidades, continuam disponíveis no bronze; a tabela final mantém as três variáveis climáticas solicitadas pelo projeto.
- **Valores ausentes:** são preservados como nulos, inclusive na chuva.
- **Carga recente:** o projeto já passa o ID retornado por `insert_one` para a transformação. Essa leitura recupera exatamente a carga da própria execução, mesmo que outra execução grave dados depois. Por isso, foi mantida em vez de acrescentar uma busca global pela maior data. O método de leitura da coleção completa continua disponível para consultar o histórico.
- **Dashboard:** consulta somente a tabela tratada no Neon, com a conexão do `config.py`. Não chama a API nem consulta o MongoDB.

O notebook `notebooks/teste.ipynb` é um experimento anterior com SQLite e não participa do pipeline entregue. Use `run_etl.py` e os módulos de `src` para executar a versão atual.

## Conferência da carga

Depois de rodar o pipeline, confira as quantidades nos logs. Para Recife, de 01/01/2024 a 07/01/2024, são esperadas 168 medições no documento bruto, na transformação e no Neon para esse mesmo recorte. Um documento no Mongo contém vários horários; não compare a quantidade de documentos com a quantidade de linhas SQL.

No shell do MongoDB conectado ao Atlas, consulte a carga mais recente de Recife:

```javascript
use cidade_inteligente
const carga = db.clima_recife.findOne({}, {}, {sort: {data_extracao: -1}})
carga.data_extracao
carga.hourly.time.length
carga.hourly.rain.filter(valor => valor === null).length
```

Se a coleção estiver vazia, rode o pipeline antes dessas consultas. Confira também `temperature_2m` e `relative_humidity_2m` no documento. Para visualizar as datas do histórico:

```javascript
db.clima_recife.find({}, {data_extracao: 1}).sort({data_extracao: -1})
```

No SQL Editor do Neon, confira o mesmo período:

```sql
SELECT COUNT(*) AS medicoes,
       COUNT(*) FILTER (WHERE temperatura IS NULL) AS temperatura_ausente,
       COUNT(*) FILTER (WHERE umidade IS NULL) AS umidade_ausente,
       COUNT(*) FILTER (WHERE chuva IS NULL) AS chuva_ausente
FROM historico_clima
WHERE cidade = 'recife'
  AND data_hora >= '2024-01-01'
  AND data_hora < '2024-01-08';
```

As contagens de ausências devem corresponder às do documento bruto usado nessa execução. Se executar outra cidade ou período, ajuste a coleção e o filtro SQL. Repetir a mesma carga mantém 168 linhas nesse recorte do Neon e acrescenta um documento ao histórico do Atlas.

## Dashboard e entrega

Após concluir a carga, execute na raiz de `Smart_City`:

```powershell
python -m streamlit run dashboard/app.py
```

Abra o endereço local mostrado no terminal. O dashboard apresenta:

- seleção entre as cidades efetivamente carregadas no Neon;
- filtro por intervalo de datas;
- temperatura média do período em `st.metric`, ignorando valores ausentes;
- gráfico de temperatura por hora e tabela das medições.

As consultas ficam em cache por até 10 minutos. Se rodar o pipeline novamente e quiser ver imediatamente a atualização, interrompa o Streamlit com `Ctrl+C` e execute o comando novamente. Quando todas as temperaturas do período forem ausentes, o indicador mostra `Sem dados`.

Para a entrega, faça um print do dashboard com os filtros, o indicador e o gráfico visíveis, e envie junto com o link do repositório contendo o código e este README atualizado. O print deve ser feito após executar e conferir os dados reais.

Os comandos de instalação, testes, pipeline, consultas e dashboard devem ser executados manualmente pelo responsável pelo projeto. A implementação dessas adequações não inclui execução dos serviços nem validação de ponta a ponta.

## Critérios atendidos

- API pública relacionada a Smart Cities;
- código Python organizado nas classes `Extract`, `Transform` e `Load`;
- dados brutos persistidos no banco NoSQL MongoDB Atlas;
- leitura do documento diretamente do Atlas antes da transformação;
- transformação com pandas;
- dados tratados persistidos no banco relacional PostgreSQL do NeonDB;
- conexões protegidas por variáveis de ambiente;
- carga relacional repetível sem duplicação;
- orquestração sequencial com tasks, flow, logs e retries do Prefect;
- identificação de cada carga bruta com `data_extracao` e preservação de valores ausentes;
- dashboard Streamlit conectado ao Neon.

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
- [Prefect: tasks](https://docs.prefect.io/v3/concepts/tasks)
- [Prefect: retries](https://docs.prefect.io/v3/how-to-guides/workflows/retries)
