import csv

class GBIFToExoAdapter:

    def __init__(self, id_translator, ten_ten_resolver=None, logger=None):
        self.id_translator = id_translator
        self.ten_ten_resolver = ten_ten_resolver
        self.logger = logger

    def resolve_coord_to_grid(self, x, y):
        pass

    def translate_10_10(self, gbif_data, append=None):
        grid = self.ten_ten_resolver.resolve_to_grid(float(gbif_data[22]),float(gbif_data[21]))
        retval = {
            'especie':                  self.id_translator[gbif_data[33]][0],  # Nom de la espècie
            'idspinvasora':             self.id_translator[gbif_data[33]][1],  # Id espècie invasora
            'grup':                     self.id_translator[gbif_data[33]][2],  # Grup del taxon,
            'utm_10':                   grid,
            'descripcio':               gbif_data[16],
            'data':                     gbif_data[29],
            'anyo':                     gbif_data[32],
            'autor_s':                  gbif_data[44],
            'font':                     gbif_data[36],
            'referencia':               gbif_data[43],
            'hash':                     gbif_data[0],
        }
        if append:
            for key in append:
                retval[key] = append[key]
        return retval

    def translate(self, gbif_data, append=None):
        retval = {
            'especie':          self.id_translator[gbif_data[33]][0],  # Nom de la espècie
            'idspinvasora':     self.id_translator[gbif_data[33]][1],  # Id espècie invasora
            'grup':             self.id_translator[gbif_data[33]][2],  # Grup del taxon
            'long':             gbif_data[22],  # Coordenada x de la citacio
            'lat':              gbif_data[21],  # Coordenada y de la citacio
            'data':             gbif_data[29],  # Data de la observacio
            'autor_s':          gbif_data[44],  # Autor de la cita,
            'localitat':        gbif_data[16],
            'hash':             gbif_data[0],
            'observacions':     '',
        }
        if append:
            for key in append:
                retval[key] = append[key]
        return retval

    def translate_tuple_point(self, gbif_data, id_paquet):
        try:
            retval = (
                self.id_translator[gbif_data[33]][0],   # Nom de la espècie
                self.id_translator[gbif_data[33]][1],   # Id espècie invasora
                self.id_translator[gbif_data[33]][2],   # Grup del taxon
                gbif_data[22],                          # Coordenada x de la citacio
                gbif_data[21],                          # Coordenada y de la citacio
                gbif_data[29],                          # Data de la observacio
                gbif_data[44][:254],                    # Autor de la cita,
                gbif_data[16][:254],                    # Localitat
                gbif_data[0],                           # hash
                '',                                     # observacions
                id_paquet,                              # id_paquet
            )
            return retval
        except KeyError:
            self.logger.info("Key Miss for species {0}, {1}".format(gbif_data[33], gbif_data[9]))
            return None

    def translate_tuple_grid(self, gbif_data, grid_presence, id_paquet):

        # 'descripcio': gbif_data[16],
        # 'data': gbif_data[29],
        # 'anyo': gbif_data[32],
        # 'autor_s': gbif_data[44],
        # 'font': gbif_data[36],
        # 'referencia': gbif_data[43],
        # 'hash': gbif_data[0],

        try:
            retval = (
                self.id_translator[gbif_data[33]][0],   # Nom de la espècie
                self.id_translator[gbif_data[33]][1],   # Id espècie invasora
                self.id_translator[gbif_data[33]][2],   # Grup del taxon
                grid_presence['id'][int(gbif_data[0])], # utm_10
                gbif_data[16],                          # descripcio
                gbif_data[29],                          # Data de la observacio
                gbif_data[44][:254],                    # Autor de la cita,
                gbif_data[36],                          # Font
                gbif_data[43],                          # Referencia
                gbif_data[0],                           # hash
                id_paquet,                              # id_paquet
            )
            return retval
        except KeyError:
            self.logger.info("Key Miss for species {0}, {1}".format(gbif_data[33], gbif_data[9]))
            return None

    def get_insert_params_point(self, points_file, presence_table, id_paquet):
        params = []
        key_hits = 0
        key_misses = 0
        with open(points_file) as csv_file:
            csv_reader = csv.reader(csv_file, delimiter='\t', quotechar='"')
            first = True
            for row in csv_reader:
                if first:
                    first = False
                else:
                    if row[0] not in presence_table:
                        param_row = self.translate_tuple_point(row, id_paquet)
                        if param_row:
                            params.append(param_row)
                            key_hits = key_hits + 1
                        else:
                            key_misses = key_misses + 1

        self.logger.info("Hits {0}, misses {1}".format( key_hits, key_misses ))
        return params

    def get_insert_params_10(self, grid_file, presence_table, id_paquet, grid_presence):
        params = []
        key_hits = 0
        key_misses = 0
        with open(grid_file) as csv_file:
            csv_reader = csv.reader(csv_file, delimiter='\t', quotechar='"')
            first = True
            for row in csv_reader:
                if first:
                    first = False
                else:
                    if row[0] not in presence_table:
                        param_row = self.translate_tuple_grid(row, grid_presence, id_paquet)
                        if param_row:
                            params.append(param_row)
                            key_hits = key_hits + 1
                        else:
                            key_misses = key_misses + 1

        self.logger.info("Hits {0}, misses {1}".format(key_hits, key_misses))
        return params
