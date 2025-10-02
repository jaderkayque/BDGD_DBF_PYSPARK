# Databricks notebook source
# MAGIC %md
# MAGIC ![Diagrama BEC2.jpeg](./Diagrama BEC2.jpeg "Diagrama BEC2.jpeg")
# MAGIC

# COMMAND ----------

# MAGIC %pip install dbfread openpyxl

# COMMAND ----------

from dbfread import DBF
import pandas as pd
import os
from time import time
import pyspark
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, DoubleType, StringType
import configparser as cf
from datetime import datetime
import pytz


# COMMAND ----------

class ConfigNamespace:
    def __init__(self, config: cf.ConfigParser):
        # Transforma as sections e keys do ConfigParser em atributos
        for section in config.sections():
            setattr(self, section, ConfigSection(config[section]))

class ConfigSection:
    def __init__(self, section):
        # Transforma as keys de uma section em atributos
        for key, value in section.items():
            setattr(self, key, value)

# COMMAND ----------

class Entidade:
  """
  Classe para carregar uma entidade DBF da BDGD

  Exemplos:
    BE = Entidade("Shape_QA_202408", "ES", "BE")
    CTMT = Entidade("Shape_QA_202408", "ES", "CTMT", "/Volumes/poseidon_uc/group_uc/ddpe/BDGD/")
    
    len(BE)
      output: 20
    
    A tabela pode ser acessada chamando BE.data ou apenas BE
    print(BE.data.head()) == print(BE.head())

  """
  def __init__(self, folder:str, region:str, name:list, root="/Volumes/poseidon_uc/group_uc/ddpe/BDGD/"):
    """
    Inicializa uma entidade DBF
  
    :param folder: Diretório da base mensal da BDGD
    :param region: Região de estudo
    :param name: Nome da entidade
    :param root: Caminho para o diretório raiz da BDGD  (Default: "/Volumes/poseidon_uc/group_uc/ddpe/BDGD/")
  
    :var data: DataFrame com os dados da entidade
    """

    self.root = root
    self.f = folder
    self.r = region
    self.name = name
    self.data = self.load()

  def load(self):
    """
    Carrega os dados da entidade
    """
    # Lista os arquivos na pasta da entidade

    files = os.listdir(f"{os.path.join(self.root,self.f,self.r)}") 

    files = [f for f in files if f.startswith(self.name)]

    df = None

    parquets = []
    try:
      parquets = dbutils.fs.ls(os.path.join(self.root,'DataLake', self.f, self.r))
    except:
      pass

    modTime = 0
    for p in parquets:
      if p.name == f"{self.name}/":
        modTime = p.modificationTime
        break
    
    arquivos = dbutils.fs.ls(os.path.join(self.root, self.f, self.r))
    arq_modTime = 0
    for arquivo in arquivos:
      if arquivo.name.startswith(self.name):
        if arq_modTime < arquivo.modificationTime:
          arq_modTime = arquivo.modificationTime

    local_tz = pytz.timezone("America/Sao_Paulo")
    # Conversão do timestamp para datetime em UTC
    utc_time = datetime.fromtimestamp(arq_modTime / 1000, pytz.utc)
    utc_delta = datetime.fromtimestamp(modTime / 1000, pytz.utc)
    local_time = utc_time.astimezone(local_tz)
    local_delta = utc_delta.astimezone(local_tz)
    use_batch = True if self.name == 'UCBT' else False
    if arq_modTime > modTime:  # Carrega apenas se ainda não estiver armazenada
      print(f'DataLake atualizado em: {local_delta} e DBF atualizado em: {local_time}, iremos realizar o update do Datalake automaticamente.')
      if len(files) >= 1:
        for file in files:
          print(f'Carregando entidade {file}, origem: DBF')
          if not isinstance(df, type(None)):
            #LOGICA> Para o primeiro arquivo ou arquivo único temos que df = None, portanto é realizado a leitura do dbf e salvo no Datalake no modo overwrite para sobreescrever o arquivo anterior
            df = self.read_dbf(file, mode="overwrite", use_batch=use_batch)
            # df.write.format("delta").mode("overwrite").save(os.path.join(self.root,'DataLake', self.f, self.r, self.name))
          else:
            #LOGICA> Para o segundo aruivo, temos que df != None, portanto é realizado a leitura do dbf e salvo no Datalake no modo append para adicionar ao arquivo anterior sem sobreescrever.
            df = self.read_dbf(file, mode="append", use_batch=use_batch)
            # df.write.format("delta").mode("append").save(os.path.join(self.root,'DataLake', self.f, self.r, self.name))
      else:
        raise("Entidade não encontrada")
        return None
      df = None
      print('DataLake atualizado com sucesso.')
    else:
      print(f'Carregando entidade {self.name}, origem: DeltaTable, atualizado em: {local_delta} e DBF atualizado em: {local_time}')
    

    # Verificar se existe algum arquivo .parquet no diretório da entidade no DataLake
    parquet_dir = os.path.join(self.root, 'DataLake', self.f, self.r, self.name)
    try:
        parquet_files = dbutils.fs.ls(parquet_dir)
        has_parquet = any(f.name.lower().endswith('.parquet') for f in parquet_files)
        if has_parquet:
            return spark.read.format('delta').load(os.path.join(self.root,'DataLake', self.f, self.r, self.name))
        else:
          print(f'A entidade {self.name} não foi carregada corretamente.')
          print(f'Utilize a função Entidades.delete_parquet({self.name} para apagar o parquet ou delete a pasta diretamente do Catálogo.)')
          return None
    except:
      print(f'A entidade {self.name} não foi carregada corretamente.')
      print(f'Utilize a função Entidades.delete_parquet({self.name}) para apagar o parquet ou delete a pasta diretamente do Catálogo.')
      return None
  

  def read_dbf(self, file, use_batch=False, mode="overwrite"):

    try:
        # Caminho do arquivo DBF
        dbf_path = os.path.join(self.root, self.f, self.r, file)
        
        # Ler o arquivo DBF
        dbf_data = DBF(dbf_path, encoding="latin1", load=False)
        batch_size = 100000
        batch = []
        i=0
        i_last = 0
        # Coletar os dados e processar os tipos
        data = []

        for record in dbf_data:
            converted_record = {}
            i = i+1
            for key, value in record.items():
                if isinstance(value, (int, float)):
                    converted_record[key] = float(value)  # Converter para float
                elif value is None:
                    converted_record[key] = None  # Garantir que valores nulos sejam mantidos
                else:
                    converted_record[key] = str(value)  # Converter para string
            batch.append(converted_record) if use_batch else data.append(converted_record)

            if len(batch) >= batch_size:
              mode = "append"
              schema = StructType([
                  StructField(key, DoubleType() if isinstance(value, float) else StringType(), True)
                  for key, value in batch[0].items()
              ])
              
              df = spark.createDataFrame(batch, schema=schema)
              print(f"Salvando registros de {i-batch_size + 1} a {i}.")
              i_last = i
              df.write.format("delta").mode(mode).save(os.path.join(self.root,'DataLake', self.f, self.r, self.name))
              batch = []
        
        if use_batch:
          data = batch
          print(f"Salvando registros de {i_last + 1} a {i}.")

        # Criar esquema explicitamente para o Spark
        if data:
            schema = StructType([
                StructField(key, DoubleType() if isinstance(value, float) else StringType(), True)
                for key, value in data[0].items()
            ])
        else:
            raise ValueError("Arquivo DBF está vazio ou não possui registros.")

        # Criar DataFrame no Spark
        df = spark.createDataFrame(data, schema=schema)
        df.write.format("delta").mode(mode).save(os.path.join(self.root,'DataLake', self.f, self.r, self.name))
        return True
    
    except Exception as e:
        print(f"Ocorreu o erro: {e}")
        return False
      
    except Exception as e:
      print(f'Ocorreu o erro: {str(e)}')
      print(f'tentaremos executar novamente em 2s')
      time.sleep(2)
      return self.read_dbf(file, use_batch)
  
  def __len__(self):
    """
    Retorna a quantidade de linhas da entidade
    """
    return self.data.count()

  def __getattr__(self, item):
        """Delegar atributos ao DataFrame."""
        return getattr(self.data, item)

  def __getitem__(self, key):
      """Delegar acesso por chave ao DataFrame."""
      return self.data[key]

  def __setitem__(self, key, value):
      """Delegar atribuições ao DataFrame."""
      self.data[key] = value

  def __iter__(self):
      """Permitir iteração sobre o DataFrame."""
      return iter(self.data)





# COMMAND ----------

class Entidades:
  """
  Classe para carregar uma coleção de entidades DBF da BDGD

  Exemplos: 
  entidades = Entidades("Shape_QA_202408", "SP", ["BE", "CTMT", "UCAT"])
  entidades = Entidades("Shape_QA_202408", "SP", ["BE", "CTMT", "UCAT"], "/Volumes/poseidon_uc/group_uc/ddpe/BDGD/")

  print(entidades.BE)
  type(entidades.CTMT) == pyspark.sql.dataframe.DataFrame
  type(entidades.CTMT.data) == pyspark.sql.dataframe.DataFrame

  ### Notar que entidades.CTMT é o mesmo que entidades.CTMT.data ###
  

  """

  def __init__(self, folder:str, region:str, entities_names:list, root="/Volumes/poseidon_uc/group_uc/ddpe/BDGD/") -> list:
    """
    Inicializa uma coleção de entidades DBF
    :param root: Caminho para o diretório raiz da BDGD
    :param folder: Diretório da base mensal da BDGD
    :param region: Região de estudo
    :param entities_names: Lista de nomes das entidades
    :param force_reload: Força a atualização das entidades (Default: False)

    :var entities: Lista de entidades

    :method append: Adiciona uma entidade à coleção de entidades.
    :method remove: Remove uma entidade da coleção de entidades.
    :method update: Atualiza uma entidade da coleção de entidades.
    """
    self.root = root
    self.f = folder
    self.r = region
    self.entities_names = entities_names
    self.entities = []
    self.__load__()
    self._configuracao = cf.ConfigParser()

  def __load__(self):
    """
    Carrega as entidades
    """
    for name in self.entities_names:
      entity = Entidade(self.f, self.r, name, self.root)
      setattr(self, name, entity)
      self.entities.append(entity)

  def __iter__(self):
    """
    Permitir iteração sobre a coleção de entidades.
    """
    return iter(self.entities)
  
  def __len__(self):
    """
    Retorna a quantidade de entidades
    """
    return len(self.entities)

  @classmethod
  def config_only(cls, instance=None, root="/Volumes/poseidon_uc/group_uc/ddpe/BDGD/"):
      """
      Cria uma instância só para manipular configurações, sem carregar entidades
      """
      if instance is not None:  
        obj = cls.__new__(cls)
        obj.root = instance.root
        obj._configuracao = instance._configuracao
        obj.f = instance.f
        obj.r = instance.r
        obj.entities_names = instance.entities_names
        obj.entities = instance.entities
      else:
        obj = cls.__new__(cls)  # cria objeto sem passar pelo __init__
        obj.root = root
        obj._configuracao = cf.ConfigParser()
        obj.f = None
        obj.r = None
        obj.entities_names = []
        obj.entities = []

      return obj

  def delete_parquet(self, name:str):
    """
    Remove o parquet de uma entidade
    :param name: Nome da entidade
    """
    dbutils.fs.rm(f"{self.root}/DataLake/{self.f}/{self.r}/{name}", True)
    self.remove(name)
    print(f"Parquet {name} removido")
  
  def append(self, name:str, entity:Entidade):
    """
    Adiciona uma entidade à coleção de entidades.
    :param name: Nome da entidade
    :param entity: Entidade a ser adicionada
    """
    if name in self.entities_names:
      raise Exception(f"Entidade {name} já existe. Utilize o método update() para atualizar a entidade.")

    setattr(self, name, entity)
    self.entities.append(entity)
    self.entities_names.append(name)
    print(f"Entidade {name} adicionada")
      
  def remove(self, name:str):
    """
    Remove uma entidade da coleção de entidades.
    :param name: Nome da entidade
    """
    self.entities.remove(getattr(self, name))
    self.entities_names.remove(name)
    delattr(self, name)    
    print(f"Entidade {name} removida")
  
  def update(self, name:str, entity:Entidade):
    """
    Atualiza uma entidade da coleção de entidades.
    :param name: Nome da entidade
    :param entity: Entidade a ser atualizada
    """
    if not name in self.entities_names:
      raise Exception(f"Entidade {name} não existe")
    setattr(self, name, entity)
    self.entities[self.entities_names.index(name)] = entity
    print(f"Entidade {name} atualizada")

  

  def config(self):
    self._configuracao.read(f"{os.path.join(self.root,'BDGD.ini')}", encoding='latin-1')
    return ConfigNamespace(self._configuracao)
    
  def set_config(self, section:str, option:str, value:str):
    if not self._configuracao.has_section(section):
            self._configuracao.add_section(section)
    self._configuracao.set(section, option, value)
  
  def get_config(self, section:str, option:str):
    return self._configuracao.get(section, option)
  
  def save_config(self):
    with open(f"{os.path.join(self.root,'BDGD.ini')}", 'w', encoding='latin-1') as configfile:
      self._configuracao.write(configfile)

  def remove_config(self, section, option):
    self._configuracao.remove_option(section, option)
    self.save_config()
    
  def display_config(self):
    for section in self._configuracao.sections():
      print(f'[{section}]')
      for option in self._configuracao.options(section):
        print(f'{option} = {self.get_config(section, option)}')

  def sincronizar_arquivo(self, file_path):
    path = fr"{file_path}"
    if "\\" in path:
      itens = path.split("\\")
      file = itens[-1]
      path = path.replace("\\", "/")
    else:
      itens = path.split("/")
      file = itens[-1]
    file = file.replace(" ","_").replace(".","_").replace("-","_")
    self.set_config("ARQUIVOS", file, path)
    self.save_config()

  def visualizar_datas_Filestore(self):
    
    if not (self.f or self.r):
      raise Exception("Esta função necessita que a classe Entidades seja instanciada. Utilize entidades.config() ou conf.config_only(instancia=entidades)")
    
    files = dbutils.fs.ls(os.path.join("/Volumes/poseidon_uc/group_uc/ddpe/BDGD/",self.f, self.r))
    files = sorted(files, key=lambda x: x.modificationTime, reverse=True)
    # Configurar o fuso horário local (exemplo: Brasil)
    local_tz = pytz.timezone("America/Sao_Paulo")
    for file in files:
        # Conversão do timestamp para datetime em UTC
        utc_time = datetime.fromtimestamp(file.modificationTime / 1000, pytz.utc)
        # Ajustar para o horário local
        local_time = utc_time.astimezone(local_tz)
        print(f"Nome: {file.name}, Último update: {local_time}")


# COMMAND ----------

def visualizar_estrutura_completa_de_arquivos():
    """
    Visualiza a estrutura completa de pastas e arquivos no FileStore de forma interativa com botões para expandir e colapsar.
    """
    from IPython.display import display, HTML
    import os

    def listar_pastas(caminho, nivel=0):
        estrutura = ""
        try:
            itens = os.listdir(caminho)
            for item in sorted(itens):
                item_path = os.path.join(caminho, item)
                is_dir = os.path.isdir(item_path)
                unique_id = f"id_{hash(item_path)}"

                if is_dir:
                    estrutura += (
                        f"<div style='margin-left: {20 * nivel}px;' class='folder'>"
                        f"<span onclick=\"toggleVisibility('{unique_id}')\" "
                        f"style='cursor: pointer; color: blue;'>[+]</span> "
                        f"<b>{item}</b></div>"
                        f"<div id='{unique_id}' style='display: none;'>"
                        f"{listar_pastas(item_path, nivel + 1)}"
                        f"</div>"
                    )
                else:
                    estrutura += (
                        f"<div style='margin-left: {20 * nivel}px;' class='file'>"
                        f"|-- {item}</div>"
                    )
        except Exception as e:
            estrutura += f"<div style='margin-left: {20 * nivel}px;'>(Erro: {str(e)})</div>"
        return estrutura

    # Gera a estrutura das pastas
    estrutura_html = listar_pastas("/Volumes/poseidon_uc/group_uc/ddpe/BDGD/")

    # HTML e JavaScript para interatividade
    html_code = f"""
    <div style="font-family: monospace;">
        {estrutura_html}
    </div>
    <script>
        function toggleVisibility(id) {{
            var element = document.getElementById(id);
            var trigger = event.target;
            if (element.style.display === "none") {{
                element.style.display = "block";
                trigger.textContent = "[-]";
            }} else {{
                element.style.display = "none";
                trigger.textContent = "[+]";
            }}
        }}
    </script>
    """
    # Exibe o resultado
    return HTML(html_code)
# Chame a função


# COMMAND ----------

print("""Biblioteca importada com sucesso. 
      Classes:  Entidade, Entidades
      Métodos: visualizar_estrutura_completa_de_arquivos()

      O parametro force_reload=False é opcional e carrega os dados de forma mais rápida, mas pode gerar problemas se os dados forem atualizados no FileStore. Para forçar a atualização com os dados do FileStore utilize force_reload=True.
      
      Exemplos:
      entidades = Entidades("Shape_QA_202408", "SP", ["BE", "CTMT", "UCAT"], force_reload=True)
      BE = Entidade("Shape_QA_202408", "SP", "BE")

      print(entidades.CTMT)
      type(entidades.CTMT.data) == pyspark.sql.dataframe.DataFrame

      Para carregar a CODI utilize a função CODI(mes, ano), onde mes e ano são a data de referencia da BDGD. Exemplo:
      codi = CODI(12, 2024)

      ### Utilize o método visualizar_estrutura_completa_de_arquivos() para visualizar a estrutura completa de pastas e arquivos no FileStore.""")

# COMMAND ----------

print("Estrutura de arquivos carregados:")
visualizar_estrutura_completa_de_arquivos()

# COMMAND ----------

def CODI(mes:int, ano:int) -> pyspark.sql.DataFrame:
  caminho_excel = "/Volumes/poseidon_uc/group_uc/ddpe/BDGD/edp_codi_aneel.xlsx"
  # Lendo o arquivo Excel com pandas

  df = pd.read_excel(caminho_excel, engine="openpyxl", sheet_name="base", header=0)
  #df = df[["Empresa", "Vetor", "Tipo", "Ponto", "Nome do Ponto", "Tensão"]]

  colunas_datas = df.columns[8:]

  # Convertendo os nomes das colunas para datetime
  colunas_datas_dt = pd.to_datetime(colunas_datas, errors='coerce')

  # Data de referência fornecida pelo usuário
  ano_referencia = ano
  mes_referencia = mes 

  # Definir intervalo de 12 meses
  data_limite_inferior = pd.Timestamp(year=ano_referencia, month=mes_referencia, day=1) - pd.DateOffset(months=11)
  data_limite_superior = pd.Timestamp(year=ano_referencia, month=mes_referencia, day=1)

  # Filtrando as colunas dentro do intervalo desejado
  colunas_selecionadas = list(df.columns[:7]) + [col for col, dt in zip(colunas_datas, colunas_datas_dt) 
                                                if data_limite_inferior <= dt <= data_limite_superior]

  df_filtrado = df[colunas_selecionadas]

  # Criando os novos nomes para as colunas mensais
  nomes_mensais = [f"ENE_{str(i).zfill(2)}" for i in range(1, 13)]

  # Criando um dicionário de substituição para as colunas mensais
  mapeamento_colunas = {old: new for old, new in zip(df_filtrado.columns[7:], nomes_mensais)}

  # Renomeando as colunas no DataFrame filtrado
  df_filtrado = df_filtrado.rename(columns=mapeamento_colunas)
  df_filtrado.iloc[:, 7:] = df_filtrado.iloc[:, 7:].apply(pd.to_numeric, errors='coerce') 
  df_filtrado = df_filtrado.fillna(0)
  # Criando novo DataFrame com as colunas filtradas

  if spark.conf.get("spark.sql.execution.arrow.pyspark.enabled"):
    spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "false")
    codi = spark.createDataFrame(df_filtrado)
    spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
  else:
    codi = spark.createDataFrame(df_filtrado)
  return codi
