import settings
import psycopg2


class DataBaseFront:
    def __init__(self, settings):
        self.conn_string = "host='" + settings.db_host + "' dbname='" + settings.db_name + "' user='" + settings.db_user + "' password='" + settings.db_password + "' port='" + settings.db_port + "'"
        self.conn = psycopg2.connect(self.conn_string)
        self.cursor = self.conn.cursor()

    def load_especies_invasores(self):
        self.cursor.execute(
            """select t.id_gbif, t.nom_especie from especieinvasora t order by 2""", )
        results = self.cursor.fetchall()
        return [{'id': r[0], 'name': r[1] } for r in results]

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

    def sql_insert_citacio(self, translated_dict, id_paquet):
        try:
            self.cursor.execute(
                """
                INSERT INTO public.citacions( especie, idspinvasora, grup, data, autor_s, observacions, id_paquet, hash, origen_dades) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING id;
                """,
                (
                    translated_dict['especie'],
                    translated_dict['idspinvasora'],
                    translated_dict['grup'],
                    translated_dict['data'],
                    translated_dict['autor_s'][:254],
                    translated_dict['observacions'],
                    id_paquet,
                    translated_dict['hash'],
                    'https://www.gbif.org/publisher/7b4f2f30-a456-11d9-8049-b8a03c50a862',
                )
            )
            # print("LOGGING INSERT")
            # print("INSERT INTO public.citacions( especie, idspinvasora, grup, data, autor_s, observacions, id_paquet, hash, origen_dades) VALUES ({}, {}, {}, {}, {}, {}, {}, {}, {})".format(translated_dict['especie'],translated_dict['idspinvasora'],translated_dict['grup'],translated_dict['data'],translated_dict['autor_s'],translated_dict['observacions'],id_paquet,translated_dict['hash'],'https://www.gbif.org/publisher/7b4f2f30-a456-11d9-8049-b8a03c50a862'))
            id = self.cursor.fetchone()[0]
            print('Performed insert ' + id_paquet + ', row id ' + str(id))
            self.cursor.execute(
                """
                UPDATE public.citacions set
                    geom_4326 = st_geomfromtext( %s ,4326),
                    geom = st_transform(st_geomfromtext( %s ,4326),23031),
                    utmx =  st_x(st_transform(st_geomfromtext( %s ,4326),23031)),
                    utmy =  st_y(st_transform(st_geomfromtext( %s ,4326),23031))
                    where id=%s;
                """,
                (
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

