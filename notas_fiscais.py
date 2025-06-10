#!/usr/bin/env python
# coding: utf-8

# In[1]:

'''''
Para contextualização, este código tem como objetivo a comparação de dois relatórios distintos do SAP ERP: o relatório de 
documentos de pedidos de compras e o relatório de notas fiscais lançadas. É necessário comparar o primeiro com o segundo, 
e estando as chaves únicas presentes em ambos, posso fazer a conclusão dentre quais documentos de compras já foram lançados
e quais não foram.
Por questões de privacidade, não estarei expondo os arquivos, apenas o código utilizado.

'''

#Importação padrão da biblioteca pandas para manipulação e tratamento de dados
import pandas as pd

#Aqui é para modificar a forma que os números floats são expostos. no caso, eles não vão possuir decimais. Fiz isto porque alguns 
#valores estavam apresentando numerações científicas
pd.set_option('display.float_format', '{:.0f}'.format)


# In[2]:


#Aqui estou nomeando os arquivos da transação KSB1 
ksb1_jan_mar = "KSB1 jan-mar 2024.xlsx"
ksb1_abr_jul = "KSB1 abr-jul 2024.xlsx"
ksb1_ago_out = "KSB1 ago-out 2024.xlsx"
ksb1_nov_dez = "KSB1 nov-dez 2024.xlsx"
ksb1_jan_mar_25 = "KSB1 jan-mar 2025.xlsx"
ksb1_abr_jun_25 = "KSB1 abr-jun 2025 (a).xlsx"


# In[3]:


#Criando dataframes a partir da leitura dos arquivos Excel que foram declarados acima
df1 = pd.read_excel(ksb1_jan_mar)
df2 = pd.read_excel(ksb1_abr_jul)
df3 = pd.read_excel(ksb1_ago_out)
df4 = pd.read_excel(ksb1_nov_dez)
df5 = pd.read_excel(ksb1_jan_mar_25)
df6 = pd.read_excel(ksb1_abr_jun_25)


# In[4]:


#Concatenando todos os dataframes em um só
df_final = pd.concat([df1, df2, df3, df4, df5, df6])


# In[5]:


#Lendo os arquivos do ME80FN
me80fn_sem1_2024 = "ME80FN SEMESTRE 1 2024.xlsx"
me80fn_sem2_2024 = "ME80FN SEMESTRE 2 2024.xlsx"
me80fn_sem1_2025 = "ME80FN SEMESTRE 1 2025.xlsx"


# In[6]:


#Criação dos dataframes
df4 = pd.read_excel(me80fn_sem1_2024)
df5 = pd.read_excel(me80fn_sem2_2024)
df6 = pd.read_excel(me80fn_sem1_2025)


# In[7]:


#Concatenando os dataframes em um só
df_final_2 = pd.concat([df4, df5, df6])

#Mudando o tipo de dado da coluna "Valor líquido pedido" para float e "Entrado em" em para data
df_final_2['Valor líquido pedido'] = df_final_2['Valor líquido pedido'].astype(float)
df_final['Entrado em'] = pd.to_datetime(df_final['Entrado em'])


# In[8]:


#Aqui eu filtrei o dataframe principal, o do ME80FN, para que nele só tenham resultados
#relacionados ao que foi criado pelo Job Run
df_final_2 = df_final_2[df_final_2["Criado por"] == "JOB_RUN"]

#Essa variável recebe a contagem distinta dos documentos de compras
num_documentos_unicos = df_final_2['Documento de compras'].nunique()

#Aqui estou criando uma nova coluna chamada Lançado_ME80FN, que se trata de um boolean fazendo referência
#à existência da mesma chave nos dois dataframes
df_final_2['Lançado_ME80FN'] = df_final_2['Chave'].isin(df_final['Chave'])

#Aqui é como se fosse um join do SQL, no caso fazemos a comparação das chaves e adiciono o "Entrado em"
df_final_2 = df_final_2.merge(
    df_final[['Chave', 'Entrado em']],
    on='Chave',
    how='left'
)

#No merge, é comum que ocorram chaves duplicadas. Por conta disto, eu dropei qualquer possível registro 
#duplicado baseado na chave
df_final_2 = df_final_2.drop_duplicates(subset=['Chave'])


# In[9]:


#Converter a coluna em data
df_final_2['Data do documento'] = pd.to_datetime(df_final_2['Data do documento'])

#Cria coluna de período mensal
df_final_2['AnoMes_real'] = df_final_2['Data do documento'].dt.to_period('M').dt.to_timestamp()


# In[10]:


#Essa linha é responsável por deixar os registros temporais como "May-25, Jan-24"
df_final_2['AnoMes'] = df_final_2['AnoMes_real'].dt.strftime('%b-%y')

#Esse trecho conta documentos únicos por mês
docs_unicos_mes = df_final_2.groupby('AnoMes')['Documento de compras'].nunique().reset_index()
docs_unicos_mes.rename(columns={'Documento de compras': 'Pedidos de Compras'}, inplace=True)


# In[11]:

#Aqui eu crio uma tabela agregada com o intuito de resumir melhor as informações, como uma pivot table.
#Eu usei o mês e o "Lançado_ME80FN", a coluna responsável por segregar as notas que foram lançadas das que não foram
pivot = df_final_2.groupby(['AnoMes', 'Lançado_ME80FN']).agg(
    Valor_Liquido=('Valor líquido pedido', 'sum'),
    Pedidos=('Documento de compras', 'count')
).reset_index()


# In[12]:

#Aqui eu transformo a tabela acima em uma literal pivot table. O AnoMes é o index e o Lançado_ME80FN será espalhado por todas as colunas
pivot_table = pivot.pivot(index='AnoMes', columns='Lançado_ME80FN')

#Essa parte é um pouco mais complicada em minha opinião. Antes da linha abaixo, a pivot table estava estruturada em multi-index, tendo ele
#dois níveis. Aqui, estou percorrendo cada um dos cabeçalhos das colunas, verificando qual o nome delas e vendo se elas têm o valor false 
#ou true
pivot_table.columns = [
    'Valor Pendente' if col[1] == False and col[0] == 'Valor_Liquido' else
    'Valor Concluído' if col[1] == True and col[0] == 'Valor_Liquido' else
    'Pedidos Pendentes' if col[1] == False and col[0] == 'Pedidos' else
    'Pedidos Concluídos'
    for col in pivot_table.columns
]

pivot_table = pivot_table.reset_index()

#Aqui estou transformando as Strings de data (como Jan-25) para um datetime do formato mês-ano. Em seguida, ordeno a tabela a partir disto
pivot_table['AnoMes_real'] = pd.to_datetime(pivot_table['AnoMes'], format='%b-%y')
pivot_table = pivot_table.sort_values('AnoMes_real')

#Abaixo estou criando 4 novas colunas, baseadas em cálculos feitos a partir de colunas já existentes. Por exemplo, Valor Total é o cálculo
#de Valor Concluído e Valor Pendente. o fillna preenche possíveis valores ausentes com o número 0
pivot_table['Valor Total'] = (
    pivot_table['Valor Concluído'].fillna(0) + 
    pivot_table['Valor Pendente'].fillna(0)
)

pivot_table['Pedidos Totais'] = (
    pivot_table['Pedidos Concluídos'].fillna(0) + 
    pivot_table['Pedidos Pendentes'].fillna(0)
)

#Esse {:.1%} serve para formatar em porcentagem de uma casa decimal
pivot_table['% Pedidos Concluídos'] = (
    pivot_table['Pedidos Concluídos'] / pivot_table['Pedidos Totais']
).fillna(0).map('{:.1%}'.format)

pivot_table['% Pedidos Pendentes'] = (
    pivot_table['Pedidos Pendentes'] / pivot_table['Pedidos Totais']
).fillna(0).map('{:.1%}'.format)

#Aqui eu faço a seleção das colunas
pivot_table = pivot_table[[
    'AnoMes',
    'Valor Total', 'Pedidos Totais',
    'Valor Concluído', 'Pedidos Concluídos', '% Pedidos Concluídos',
    'Valor Pendente', 'Pedidos Pendentes', '% Pedidos Pendentes',
    'AnoMes_real'
]]

#Aqui eu faço a ordenação das colunas e apago a coluna auxiliar que gerou tais ordenações em seguida
pivot_table = pivot_table.sort_values('AnoMes_real')
pivot_table = pivot_table.drop(columns='AnoMes_real')
pivot_table


# In[13]:

#Aqui junta a tabela principal com a contagem de documentos únicos por mês
pivot_table = pivot_table.merge(docs_unicos_mes, on='AnoMes', how='left')

# Extrai apenas o ano e o mês das colunas abaixo
df_final_2['MesDoc'] = df_final_2['Data do documento'].dt.to_period('M')
df_final_2['MesEntrado'] = df_final_2['Entrado em'].dt.to_period('M')

# Abaixo foi feia uma função que classifica se o documento foi lançado no mesmo mês, em meses posteriores ou anteriores
def classificar_relacao(row):
    if row['MesDoc'] == row['MesEntrado']:
        return 'Mesmo mês'
    elif row['MesDoc'] < row['MesEntrado']:
        return 'Doc antigo'
    else:
        return 'Doc futuro'

#Aqui é para executar a função acima em cada linha
df_final_2['Relação Doc-Entrado'] = df_final_2.apply(classificar_relacao, axis=1)

# Agrupar e pivotar novamente
relacao = df_final_2.groupby(['AnoMes', 'Relação Doc-Entrado']).agg(
    Valor=('Valor líquido pedido', 'sum'),
    Pedidos=('Documento de compras', 'count')
).reset_index()

#Abaixo, confesso que necessitei de ajuda, e ainda assim fiquei um pouco confuso de início. 
# Basicamente, cria uma tabela dinâmica e junta os nomes das colunas em uma string única, como "Pedidos Doc antigo".
relacao_pivot = relacao.pivot(index='AnoMes', columns='Relação Doc-Entrado')
relacao_pivot.columns = [' '.join(col).strip() for col in relacao_pivot.columns.values]
relacao_pivot = relacao_pivot.reset_index()

# Juntar com a pivot table principal
pivot_table = pivot_table.merge(relacao_pivot, on='AnoMes', how='left')

# Para cada categoria, calcula o percentual sobre o total de pedidos
for categoria in ['Mesmo mês', 'Doc antigo', 'Doc futuro']:
    col_pedidos = f'Pedidos {categoria}'
    if col_pedidos in pivot_table.columns:
        pivot_table[f'% Pedidos {categoria}'] = (
            pivot_table[col_pedidos] / pivot_table['Pedidos Totais']
        ).fillna(0).map('{:.1%}'.format)


# In[14]:

#Ordenação das colunas
pivot_table = pivot_table[[
    'AnoMes',
    'Valor Total',
    'Pedidos de Compras',
    'Pedidos Totais',
    'Valor Concluído',
    'Pedidos Concluídos',
    '% Pedidos Concluídos',
    'Valor Pendente',
    'Pedidos Pendentes',
    '% Pedidos Pendentes',
    'Valor Doc antigo',
    'Pedidos Doc antigo',
    '% Pedidos Doc antigo',
    'Valor Doc futuro',
    'Pedidos Doc futuro',
    '% Pedidos Doc futuro',
    'Valor Mesmo mês',
    'Pedidos Mesmo mês',
    '% Pedidos Mesmo mês'
]]

#Criação e exportação da tabela final em um arquivo xlsx (ou excel). Fim!
pivot_table.to_excel("tabela_dinamica.xlsx", index=False)
print("Arquivo Exportado com Sucesso!")