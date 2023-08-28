INSERT INTO NGrama (SistemaLegado, Instancia, InteractionId, DataIntegracao, Unigrama, Bigrama, Trigrama, DataInsercao,
DataAlteracao) SELECT SistemaLegado, Instancia, InteractionId, DataIntegracao, Unigrama, Bigrama, Trigrama, NOW(),
NOW() FROM _NGrama WHERE NOT EXISTS (SELECT 1 FROM NGrama WHERE NGrama.SistemaLegado = _NGrama.SistemaLegado AND
NGrama.Instancia = _NGrama.Instancia AND NGrama.InteractionId = _NGrama.InteractionId);
