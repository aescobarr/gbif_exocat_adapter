

class GBIFToExoAdapter:

    def __init__(self, id_translator):
        self.id_translator = id_translator

    def translate(self, gbif_data):
        return {
            'especie':          self.id_translator[gbif_data[33]][0],  # Nom de la espècie
            'idspinvasora':     self.id_translator[gbif_data[33]][1],  # Id espècie invasora
            'grup':             self.id_translator[gbif_data[33]][2],  # Grup del taxon
            'long':             gbif_data[22],  # Coordenada x de la citacio
            'lat':              gbif_data[21],  # Coordenada y de la citacio
            'data':             gbif_data[29],  # Data de la observacio
            'autor_s':          gbif_data[44],  # Autor de la cita,
            'localitat':        gbif_data[16],
            'hash':             gbif_data[0],
            'observacions':     ''
        }