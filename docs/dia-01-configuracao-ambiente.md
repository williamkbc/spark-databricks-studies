# Dia 1 - Configuracao do Ambiente Spark com Docker

**Data:** 2026-01-24
**Aluno:** Will
**Ambiente:** VPS Hostinger (Ubuntu 25.10, 2 CPU, 8GB RAM, 100GB disco)

---

## Objetivos Alcancados

- [x] Entender a arquitetura de um cluster Spark
- [x] Configurar ambiente Docker na VPS
- [x] Subir cluster Spark com Master + Worker
- [x] Validar acesso às interfaces web via SSH Tunnel
- [x] Executar primeiro job PySpark (calculo de Pi)
- [x] Diagnosticar e resolver problema de permissoes no Worker

---

## Conceitos Aprendidos

### 1. Arquitetura Spark Standalone

```
+-------------------------------------------------------------+
|                     CLUSTER SPARK                           |
|                                                             |
|  +------------------+                                       |
|  |   Spark Master   | <--- Coordena trabalhos               |
|  |     :7077        |      Distribui tasks para workers     |
|  |     :8080 (UI)   |      Monitora estado do cluster       |
|  +--------+---------+                                       |
|           |                                                 |
|           v                                                 |
|  +------------------+                                       |
|  |   Spark Worker   | <--- Executa tasks em paralelo        |
|  |   172.18.0.6     |      Reporta status ao Master         |
|  |   2 cores, 2GB   |      Armazena dados em memoria        |
|  +------------------+                                       |
|                                                             |
|  +------------------+                                       |
|  |  History Server  | <--- Persiste historico de jobs       |
|  |     :18080       |      Permite analise post-mortem      |
|  +------------------+                                       |
+-------------------------------------------------------------+
```

**Por que importa:**
- Em producao (Databricks, EMR, Dataproc), a infraestrutura e abstraida
- Entender a arquitetura ajuda a debugar problemas de performance
- Configuracoes como cores e memoria impactam diretamente o custo

### 2. Docker e Containerizacao

| Conceito | Definicao | Aplicacao no Spark |
|----------|-----------|-------------------|
| **Container** | Ambiente isolado e portavel | Cada componente Spark roda em container separado |
| **Image** | Template imutavel do container | `owshq-spark:3.5` com todas dependencias |
| **Volume** | Persistencia de dados | Jobs, storage, logs montados do host |
| **Network** | Comunicacao entre containers | Master e Workers na mesma rede Docker |

### 3. Modos de Execucao Spark

| Modo | Comando | Uso |
|------|---------|-----|
| **Local** | `--master local[2]` | Desenvolvimento, testes rapidos |
| **Standalone** | `--master spark://master:7077` | Cluster proprio |
| **YARN** | `--master yarn` | Hadoop clusters |
| **Kubernetes** | `--master k8s://...` | Ambientes cloud-native |

---

## Arquiteturas Implementadas

### Ambiente Docker Compose

```yaml
services:
  spark-master:      # Coordenador do cluster
    image: apache/spark:3.5.1
    ports: [8080, 7077]

  spark-worker:      # Executor de tasks
    image: apache/spark:3.5.1
    environment:
      SPARK_WORKER_MEMORY: 2G
      SPARK_WORKER_CORES: 1

  jupyter:           # Desenvolvimento interativo
    image: pyspark-notebook
    ports: [8888]

  postgres:          # Banco de dados para exercicios
    image: postgres:15-alpine
    ports: [5432]

  minio:             # Object storage (simula S3)
    image: minio/minio
    ports: [9000, 9001]
```

### Fluxo de Execucao de um Job

```
1. spark-submit envia job ao Master
         |
         v
2. Master agenda tasks nos Workers disponiveis
         |
         v
3. Worker executa tasks em paralelo (executor)
         |
         v
4. Resultados retornam ao Driver
         |
         v
5. Job finaliza, logs vao para History Server
```

---

## Comandos Executados

### Configuracao Inicial
```bash
# Clone do repositorio via SSH
git clone git@github.com:owshq-academy/frm-spark-databricks-mec.git

# Configuracao do .env
cat > .env << 'EOF'
APP_SRC_PATH=/home/will/frm-spark-databricks-mec/src
APP_STORAGE_PATH=/home/will/frm-spark-databricks-mec/storage
APP_LOG_PATH=/home/will/frm-spark-databricks-mec/build/logs
APP_METRICS_PATH=/home/will/frm-spark-databricks-mec/build/metrics
EOF

# Criacao de diretorios
mkdir -p logs metrics
```

### Build de Imagens Customizadas
```bash
# Imagem Spark com dependencias Python
docker build -t owshq-spark:3.5 -f Dockerfile.spark.local .

# Imagem History Server
docker build -t owshq-spark-history-server:3.5 -f Dockerfile.history.local .
```

### Correcao de Permissoes (problema encontrado)
```bash
# Worker nao conseguia criar diretorio de trabalho
docker exec -u root spark-worker mkdir -p /opt/spark/work
docker exec -u root spark-worker chmod -R 777 /opt/spark/work
docker exec -u root spark-worker chown -R spark:spark /opt/spark/work
```

### Execucao de Jobs
```bash
# Modo local (teste rapido)
docker exec -it spark-master /opt/spark/bin/spark-submit \
  --master local[2] \
  /opt/spark/examples/src/main/python/pi.py 10

# Modo cluster (distribuido)
docker exec -it spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark/examples/src/main/python/pi.py 10
```

### Acesso via SSH Tunnel (Windows)
```powershell
ssh -L 8080:localhost:8080 -L 18080:localhost:18080 -L 8888:localhost:8888 will@31.97.162.147
```

---

## Erros e Correcoes

| Erro Encontrado | Causa Raiz | Correcao Aplicada | Licao Aprendida |
|-----------------|------------|-------------------|-----------------|
| `bitnami/spark:3.5 not found` | Imagem indisponivel no Docker Hub | Usar `apache/spark:3.5.1` local | Sempre verificar imagens disponiveis com `docker images` |
| `pandas==2.2.2 not found` | Incompatibilidade com Python da imagem | Usar `pandas==2.1.4` | Verificar compatibilidade de versoes |
| `Failed to create directory /opt/spark/work` | Permissao negada no worker | `chmod -R 777 /opt/spark/work` | Containers podem ter restricoes de permissao |
| `SparkContext was shut down` | Worker nao conseguia executar | Corrigir permissoes | Sempre verificar logs do worker ao debugar |

---

## Boas Praticas Aplicadas

### 1. Usar SSH Tunnel ao inves de expor portas
**Por que:** Seguranca - nao expoe servicos diretamente a internet

```bash
# Seguro: acesso via tunnel
ssh -L 8080:localhost:8080 user@server

# Inseguro: abrir firewall
sudo ufw allow 8080/tcp  # Evitar em producao
```

### 2. Verificar logs de todos os componentes
**Por que:** O erro pode estar em componente diferente do que parece

```bash
docker logs spark-master  # Driver
docker logs spark-worker  # Executor (onde estava o erro!)
```

### 3. Testar em modo local antes do cluster
**Por que:** Isola problemas de codigo vs infraestrutura

```bash
# Se local funciona e cluster nao = problema de infra
--master local[2]   # Teste local
--master spark://   # Depois cluster
```

### 4. Usar imagens locais quando disponiveis
**Por que:** Evita dependencia de registries externos, mais rapido

```bash
docker images | grep spark  # Ver o que ja tem
```

---

## Exercicios Resolvidos

### Exercicio 1: Calculo de Pi com Monte Carlo

**Problema:** Validar que o cluster Spark esta funcionando executando um job distribuido

**Solucao:**
```bash
docker exec -it spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --name "Teste-Cluster" \
  /opt/spark/examples/src/main/python/pi.py 10
```

**Resultado:**
- Pi ~ 3.135920
- 10 tasks executadas em paralelo
- Job completou em 6.597s
- exitCode 0 (sucesso)

**Analise:**
- O algoritmo Monte Carlo gera pontos aleatorios e calcula quantos caem dentro de um circulo
- Mais particoes (argumento 10) = mais precisao, mas mais overhead
- Tasks foram distribuidas para o executor no worker 172.18.0.6

---

## Metricas do Dia

| Metrica | Valor |
|---------|-------|
| Tempo total de setup | ~2 horas |
| Imagens Docker criadas | 2 (2.72GB cada) |
| Containers rodando | 5 (master, worker, jupyter, postgres, minio) |
| Jobs executados | 3 (2 locais, 1 cluster) |
| Erros encontrados | 4 |
| Erros resolvidos | 4 |

---

*Documento gerado como parte do curso de formacao Spark e Databricks - Engenharia de Dados Academy*
