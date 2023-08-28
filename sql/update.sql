UPDATE NGrama, _NGrama SET NGrama.Unigrama = _NGrama.Unigrama, NGrama.Bigrama = _NGrama.Bigrama, NGrama.Trigrama = _NGrama.Trigrama,
NGrama.DataAlteracao = NOW() WHERE NGrama.SistemaLegado = _NGrama.SistemaLegado AND NGrama.Instancia = _NGrama.Instancia AND
NGrama.InteractionId = _NGrama.InteractionId;