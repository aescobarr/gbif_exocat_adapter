

class GBIFToExoAdapter:

    def __init__(self, id_translator, ten_ten_resolver=None):
        self.id_translator = id_translator
        self.ten_ten_resolver = ten_ten_resolver

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
