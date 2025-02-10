'''
Para un usuario dado, por ejemplo "jgonzal" sacar todos los tickets que no tienen a nadie asignado 
en alguno de los grupos a los que pertence "jgonzal"
'''
import psycopg2
import pandas
conn = psycopg2.connect(database = 'postgres', user = 'postgres', host = 'localhost', password = 'python', port = 5432)
cur = conn.cursor()
consulta = '''
select distinct
	a.ticket_name as ticket,
	b.group_name as grupo,
	a.ticket_user_id_vinculation_type as tipo_de_asignación
from 
	glpi_tickets_tasks_projects_202501301345 a
left join
	glpi_users_groups_202501301353 b
on
	a.ticket_group_id_vinculation = b.group_id
where 
	b.group_name
in
(select 
	group_name 
from 
	glpi_users_groups_202501301353 
where 
	user_name = 'jgonzal' )
and 
	a.ticket_user_id_vinculation = 0 
and 
	b.user_name = 'jgonzal' 
and 
	ticket_state_status != 'Finalizado'
'''
tabla = pandas.read_sql(consulta, conn)
print(tabla)
cur.close()
conn.close()