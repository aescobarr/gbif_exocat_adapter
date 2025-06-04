import settings
import psycopg2
import csv
from io import StringIO
from psycopg2.extras import execute_values


class DataBaseFront:
    def __init__(self, settings, logger):
        self.conn_string = "host='" + settings.db_host + "' dbname='" + settings.db_name + "' user='" + settings.db_user + "' password='" + settings.db_password + "' port='" + settings.db_port + "'"
        self.conn = psycopg2.connect(self.conn_string)
        self.cursor = self.conn.cursor()
        self.logger = logger

    def row_already_exists_10_10(self, hash):
        self.cursor.execute(
            """
            SELECT hash FROM public.citacions_10 WHERE hash=%s
            """,
            (hash,)
        )
        res = self.cursor.fetchone()
        if res is not None:
            if len(res) > 0:
                return True
        return False

    def row_already_exists(self, hash):
        self.cursor.execute(
            """
            SELECT hash FROM public.citacions WHERE hash=%s
            """,
            (hash,)
        )
        res = self.cursor.fetchone()
        if res is not None:
            if len(res) > 0:
                return True
        return False

    def resolve_to_grid(self, x, y):
        self.cursor.execute(
            """
            select code_utm_1 from quad_temp where resolution = 10000 and st_contains(st_transform(geom, 4326), st_geomfromtext('POINT (%s %s)', 4326));
            """,
            (x, y)
        )
        result = self.cursor.fetchone()
        if result:
            return result[0]
        else:
            return None

    def load_grid_resolver_tables(self):
        self.cursor.execute(
            """
            select code_utm_1, st_xmin(st_transform(geom, 4326)) as xmin, st_xmax(st_transform(geom, 4326)) from quad_temp where resolution = 10000 order by st_xmin(st_transform(geom, 4326));
            """,
        )
        x_table = self.cursor.fetchall()
        self.cursor.execute(
            """
            select code_utm_1, st_ymin(st_transform(geom, 4326)) as ymin, st_ymax(st_transform(geom, 4326)) from quad_temp where resolution = 10000 order by st_xmin(st_transform(geom, 4326));
            """
        )
        y_table = self.cursor.fetchall()
        return [x_table, y_table]

    def load_especies_invasores(self):
        self.cursor.execute(
            """select t.id_gbif, t.nom_especie from especieinvasora t where t.id_gbif is not null order by 2""", )
        results = self.cursor.fetchall()
        return [{'id': r[0], 'name': r[1] } for r in results]

    def load_reverse_taxon_resolution_data(self):
        self.cursor.execute(
            """
                select distinct t.nom_especie, t.id, g2.id, t.id_gbif
                from especieinvasora t,
                grupespecie g,
                grup g2
                where t.id = g.idespecieinvasora
                and g.idgrup = g2.id
                and t.id_gbif is not null
                order by 1
            """, )
        results = self.cursor.fetchall()
        return [ [r[0],r[1],r[2],r[3]] for r in results]

    def load_taxons(self):
        retval = []
        self.cursor.execute(
            """select t.id, t.scientificname from taxon t order by 2""", )
        results = self.cursor.fetchall()
        for result in results:
            retval.append({ 'id': result[0], 'scientificname': result[1]})
        return retval

    def load_missing_gbif_taxons(self):
        retval = []
        self.cursor.execute(
            """select foo.id, foo.scientificname from (select t.id, t.scientificname, t.gbif_id from especieinvasora ei,taxon t where ei.idtaxon = t.id) as foo where foo.gbif_id is NULL order by 2""", )
        results = self.cursor.fetchall()
        for result in results:
            retval.append({ 'id': result[0], 'scientificname': result[1]})
        return retval

    def load_live_taxons(self):
        retval = {}
        self.cursor.execute("""select i.id as idexocat, concat(genere,' ' || especie,' ' || subespecie,' ' || varietat,' ' || subvarietat,' ' || forma), g.nom from taxon t, especieinvasora i, grupespecie ge, grup g WHERE i.idtaxon = t.id AND ge.idespecieinvasora = i.id AND g.id = ge.idgrup order by 2""",)
        results = self.cursor.fetchall()
        for result in results:
            retval[result[1]] = [result[0],result[2]]
        return retval

    def sql_update_citacio_10_10(self, translated_dict, id_paquet):
        self.cursor.execute(
            """
        UPDATE public.citacions_10 set especie=%s,idspinvasora=%s,grup=%s,utm_10=%s,descripcio=%s,data=%s,anyo=%s,autor_s=%s,font=%s,referencia=%s,id_paquet=%s
        where hash=%s;
        """,
        (
            translated_dict['especie'],
            translated_dict['idspinvasora'],
            translated_dict['grup'],
            translated_dict['utm_10'],
            translated_dict['descripcio'],
            translated_dict['data'],
            translated_dict['anyo'],
            translated_dict['autor_s'][:254],
            translated_dict['font'],
            translated_dict['referencia'],
            id_paquet,
            translated_dict['hash']
        ))
        self.conn.commit()

    def sql_update_citacio(self, translated_dict, id_paquet):
        self.cursor.execute(
            """
            UPDATE public.citacions set especie=%s, idspinvasora=%s, grup=%s, data=%s, autor_s=%s, observacions=%s,
            id_paquet=%s,origen_dades=%s, localitat=%s, citacio=%s
            where hash=%s;
            """,
            (
                translated_dict['especie'],
                translated_dict['idspinvasora'],
                translated_dict['grup'],
                translated_dict['data'],
                translated_dict['autor_s'][:254],
                translated_dict['observacions'],
                id_paquet,
                'https://www.gbif.org/',
                translated_dict['localitat'][:254],
                translated_dict['citacio'],
                translated_dict['hash'],
            )
        )
        self.cursor.execute(
            """
            UPDATE public.citacions set
                geom_4326 = st_geomfromtext( %s ,4326),
                geom = st_transform(st_geomfromtext( %s ,4326),23031),
                utmx =  st_x(st_transform(st_geomfromtext( %s ,4326),23031)),
                utmy =  st_y(st_transform(st_geomfromtext( %s ,4326),23031))
                where hash=%s;
            """,
            (
                'POINT({0} {1})'.format(translated_dict['long'], translated_dict['lat']),
                'POINT({0} {1})'.format(translated_dict['long'], translated_dict['lat']),
                'POINT({0} {1})'.format(translated_dict['long'], translated_dict['lat']),
                'POINT({0} {1})'.format(translated_dict['long'], translated_dict['lat']),
                translated_dict['hash'],
            )
        )
        self.cursor.execute(
            """
            UPDATE public.citacions set
            geom_25831 = st_transform(geom_4326, 25831)
            where hash=%s;
            """,
            (
                translated_dict['hash'],
            )
        )
        self.conn.commit()

    def sql_insert_citacio_10_10(self, translated_dict, id_paquet):
        try:
            self.cursor.execute(
                """
                INSERT INTO public.citacions_10( especie, idspinvasora, grup, utm_10, descripcio, data, anyo, autor_s, font, referencia, hash, id_paquet) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
                """,
                (
                    translated_dict['especie'],
                    translated_dict['idspinvasora'],
                    translated_dict['grup'],
                    translated_dict['utm_10'],
                    translated_dict['descripcio'],
                    translated_dict['data'],
                    translated_dict['anyo'],
                    translated_dict['autor_s'][:254],
                    translated_dict['font'],
                    translated_dict['referencia'],
                    translated_dict['hash'],
                    id_paquet,
                )
            )
            try:
                self.cursor.execute(
                """
                    INSERT INTO public.presencia_sp(idquadricula, idspinvasora) VALUES (%s, %s)
                    """,
            (
                    translated_dict['utm_10'],
                    translated_dict['idspinvasora'])
                )
            except psycopg2.IntegrityError:
                pass
            self.conn.commit()
        except psycopg2.InternalError as e:
            print(e)
        except psycopg2.DataError as e:
            print(e)
            print(translated_dict)

    def sql_update_grid_bulk(self, update_params_set):
        # especie, idspinvasora, grup, utm_10, descripcio, data, autor_s, font, referencia, id_paquet
        self.logger.info("Bulk updating citacions...")
        records_list_template = ','.join(['%s'] * len(update_params_set))
        query = """
                UPDATE citacions_10
                SET
                especie = update_payload.especie,
                idspinvasora = update_payload.idspinvasora,
                grup = update_payload.grup,
                utm_10 = update_payload.utm_10,
                descripcio = update_payload.descripcio,
                data = update_payload.data,
                autor_s = update_payload.autor_s,
                font = update_payload.font,
                referencia = update_payload.referencia,
                id_paquet = update_payload.id_paquet
                FROM (VALUES {}) AS update_payload (especie, idspinvasora, grup, utm_10, descripcio, data, autor_s, font, referencia, id_paquet, hash)
                WHERE citacions_10.hash = update_payload.hash
                """.format(records_list_template)
        self.cursor.execute(query, update_params_set)
        self.conn.commit()

    def sql_update_citacio_bulk(self, update_params_set, id_paquet):
        self.logger.info("Bulk updating citacions...")
        records_list_template = ','.join(['%s'] * len(update_params_set))
        query = """
        UPDATE citacions
        SET
        especie = update_payload.especie,
        idspinvasora = update_payload.idspinvasora,
        grup = update_payload.grup,
        utmx = update_payload.utmx::double precision,
        utmy = update_payload.utmy::double precision,
        data = update_payload.data,
        autor_s = update_payload.autor_s,
        localitat = update_payload.localitat,
        observacions = update_payload.observacions,
        id_paquet = update_payload.id_paquet
        FROM (VALUES {}) AS update_payload (especie, idspinvasora, grup, utmx, utmy, data, autor_s, localitat, observacions, id_paquet, hash)
        WHERE citacions.hash = update_payload.hash
        """.format(records_list_template)
        self.cursor.execute(query, update_params_set)
        self.conn.commit()
        self.update_citacions_geometry(id_paquet)


    def sql_insert_citacio_bulk(self, insert_params_set, id_paquet):
        records_list_template = ','.join(['%s'] * len(insert_params_set))
        query = "INSERT INTO public.citacions( especie, idspinvasora, grup, utmx, utmy, data, autor_s, localitat, hash, observacions, id_paquet) VALUES {};".format(records_list_template)

        self.logger.info("Bulk inserting citacions...")
        self.cursor.execute(query, insert_params_set)
        self.conn.commit()
        self.logger.info("Done Bulk inserting citacions...")
        self.update_citacions_geometry(id_paquet)


    def update_citacions_geometry(self,id_paquet):
        self.logger.info("Updating citacions geometry...")
        self.cursor.execute(
            """
            UPDATE public.citacions c set 
            geom_4326 = st_geomfromtext( 'POINT(' || c.utmx || ' ' || c.utmy || ')' ,4326) 
            where id_paquet=%s;            
            """,
            (id_paquet,)
        )
        self.conn.commit()
        self.cursor.execute(
            """
            UPDATE public.citacions set
                geom = st_transform(geom_4326,23031),
                geom_25831 = st_transform(geom_4326, 25831),
                utmx =  st_x(st_transform(geom_4326,23031)),
                utmy =  st_y(st_transform(geom_4326,23031))
                where id_paquet = %s;
            """,
            (id_paquet,)
        )
        self.conn.commit()
        self.logger.info("Done updating citacions geometry...")


    def sql_insert_citacio_10_bulk(self, insert_params_set):

        # Remove duplicate records from presencia_sp
        #
        # DELETE FROM presencia_sp
        # WHERE ctid IN (
        #     SELECT ctid
        #     FROM (
        #         SELECT ctid,
        #             ROW_NUMBER() OVER (PARTITION BY idquadricula, idspinvasora ORDER BY ctid) AS rn
        #         FROM presencia_sp
        #     ) sub
        #     WHERE rn > 1
        # );

        records_list_template = ','.join(['%s'] * len(insert_params_set))
        query = "INSERT INTO public.citacions_10( especie, idspinvasora, grup, utm_10, descripcio, data, autor_s, font, referencia, hash, id_paquet) VALUES {};".format(records_list_template)
        self.logger.info("Bulk inserting citacions 10x10...")
        self.cursor.execute(query, insert_params_set)
        self.conn.commit()
        self.logger.info("Done bulk inserting citacions 10x10")

        self.logger.info("Inserting presence 10x10...")
        
        bulk_presence_params = list(set((x[3], x[1]) for x in insert_params_set))
        self.logger.info("Going to perform bulk delete/insert for")
        for params in bulk_presence_params:
            self.logger.info("{} {}".format( params[0], params[1] ))

        
        query_delete_presence = "DELETE FROM public.presencia_sp where (idquadricula,idspinvasora) in (%s)"
        execute_values( self.cursor, query_delete_presence, bulk_presence_params )
        self.conn.commit()

        query_insert_presence = "INSERT into public.presencia_sp(idquadricula, idspinvasora) values %s"
        execute_values( self.cursor, query_insert_presence, bulk_presence_params )
        self.conn.commit()
        
        self.logger.info("Done Inserting presence 10x10")        

    def sql_insert_citacio(self, translated_dict, id_paquet):
        try:
            self.cursor.execute(
                """
                INSERT INTO public.citacions( especie, idspinvasora, grup, data, autor_s, localitat, observacions, id_paquet, hash, origen_dades, citacio) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING id;
                """,
                (
                    translated_dict['especie'],
                    translated_dict['idspinvasora'],
                    translated_dict['grup'],
                    translated_dict['data'],
                    translated_dict['autor_s'][:254],
                    translated_dict['localitat'][:254],
                    translated_dict['observacions'],
                    id_paquet,
                    translated_dict['hash'],
                    'https://www.gbif.org/',
                    translated_dict['citacio'],
                )
            )
            # print("LOGGING INSERT")
            # print("INSERT INTO public.citacions( especie, idspinvasora, grup, data, autor_s, observacions, id_paquet, hash, origen_dades) VALUES ({}, {}, {}, {}, {}, {}, {}, {}, {})".format(translated_dict['especie'],translated_dict['idspinvasora'],translated_dict['grup'],translated_dict['data'],translated_dict['autor_s'],translated_dict['observacions'],id_paquet,translated_dict['hash'],'https://www.gbif.org/publisher/7b4f2f30-a456-11d9-8049-b8a03c50a862'))
            id = self.cursor.fetchone()[0]
            # print('Performed insert ' + id_paquet + ', row id ' + str(id))
            self.cursor.execute(
                """
                UPDATE public.citacions set
                    geom_4326 = st_geomfromtext( %s ,4326),
                    geom = st_transform(st_geomfromtext( %s ,4326),23031),
                    geom_25831 = st_transform(st_geomfromtext( %s ,4326), 25831),
                    utmx =  st_x(st_transform(st_geomfromtext( %s ,4326),23031)),
                    utmy =  st_y(st_transform(st_geomfromtext( %s ,4326),23031))
                    where id=%s;
                """,
                (
                    'POINT({0} {1})'.format(translated_dict['long'], translated_dict['lat']),
                    'POINT({0} {1})'.format(translated_dict['long'], translated_dict['lat']),
                    'POINT({0} {1})'.format(translated_dict['long'], translated_dict['lat']),
                    'POINT({0} {1})'.format(translated_dict['long'], translated_dict['lat']),
                    'POINT({0} {1})'.format(translated_dict['long'], translated_dict['lat']),
                    id
                )
            )
            self.conn.commit()
        except psycopg2.InternalError as e:
            print(e)
        except psycopg2.DataError as e:
            print(e)
            print(translated_dict)

    def create_temp_hash_table(self):
        self.cursor.execute("CREATE TABLE IF NOT EXISTS temp_ids (hash varchar(5450) PRIMARY KEY);")
        self.conn.commit()

    def drop_temp_hash_table(self):
        self.cursor.execute("DROP TABLE IF EXISTS temp_ids;")
        self.conn.commit()

    def load_temp_hash_table(self,filename):
        output = StringIO()
        ids_list = set()
        with open(filename, newline='', encoding='utf-8') as csvfile:
            reader = csv.reader(csvfile, delimiter='\t')
            next(reader, None)  # Skip header if it exists
            for row in reader:
                if row:
                    if row[0] not in ids_list:
                        ids_list.add(row[0])
                        output.write(f"{row[0]}\n")

        output.seek(0)  # Reset buffer position

        # Use COPY to bulk insert into the table
        self.cursor.copy_expert("COPY temp_ids(hash) FROM STDIN WITH (FORMAT csv)", output)
        self.conn.commit()

    def get_present_ids(self, file_name, table):
        self.drop_temp_hash_table()
        self.create_temp_hash_table()
        self.load_temp_hash_table(file_name)

        query_template_present = "SELECT c.id, c.hash FROM public.{0} c left join temp_ids t on c.hash = t.hash where c.hash is not null and t.hash is not null;"

        # Already in database
        query = query_template_present.format(table)
        self.cursor.execute( query, ())
        results_present = self.cursor.fetchall()
        self.conn.commit()

        retval = set()
        for r in results_present:
            retval.add( r[1] )
        return retval


