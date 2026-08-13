use super::*;

impl MewsCommands<'_> {
    pub async fn set_api_key(&self, provider: &str, key: String) -> Result<()> {
        Ok(mews_router::RouterClient::new(&self.root)
            .set_api_key(provider.to_owned(), key)
            .await?)
    }

    pub async fn set_auth(
        &self,
        provider: &str,
        credential: &mews_router::AuthCredential,
    ) -> Result<()> {
        Ok(mews_router::RouterClient::new(&self.root)
            .set_auth(provider.to_owned(), credential.clone())
            .await?)
    }

    pub async fn remove_auth(&self, provider: &str) -> Result<()> {
        Ok(mews_router::RouterClient::new(&self.root)
            .remove_auth(provider.to_owned())
            .await?)
    }

    pub async fn auth_statuses(&self) -> Result<Vec<mews_router::AuthStatus>> {
        Ok(mews_router::RouterClient::new(&self.root)
            .auth_statuses()
            .await?)
    }

    pub async fn models(&self) -> Result<Vec<crate::ModelInfo>> {
        Ok(mews_router::RouterClient::new(&self.root).models().await?)
    }

    pub async fn refresh_models(&self) -> Result<Vec<crate::ModelInfo>> {
        Ok(mews_router::RouterClient::new(&self.root)
            .refresh_models(None)
            .await?)
    }

    pub fn provider_defaults(&self) -> Result<crate::ProviderDefaults> {
        Ok(self.mews.store.provider_defaults()?)
    }

    pub async fn set_default_model(&self, model: &str) -> Result<()> {
        if !self
            .models()
            .await?
            .iter()
            .any(|candidate| candidate.id == model)
        {
            bail!("model {model:?} is absent from the catalog");
        }
        self.mews.store.set_default_model(&self.context, model)?;
        Ok(())
    }

    pub async fn set_default_reasoning(
        &self,
        reasoning: Option<crate::ReasoningEffort>,
    ) -> Result<()> {
        if reasoning == Some(crate::ReasoningEffort::Auto) {
            bail!(
                "reasoning auto is not supported by the native mews Harness; use Provider default instead"
            );
        }
        if let Some(reasoning) = reasoning {
            let defaults = self.provider_defaults()?;
            let default_model = defaults.model.context(
                "no default model is configured; select one with `mews providers models`",
            )?;
            let model = self
                .models()
                .await?
                .into_iter()
                .find(|model| model.id == default_model)
                .with_context(|| {
                    format!("default model {} is absent from the catalog", default_model)
                })?;
            if !model.reasoning.contains(&reasoning) {
                bail!("reasoning {reasoning:?} is not supported by {}", model.id);
            }
        }
        self.mews
            .store
            .set_default_reasoning(&self.context, reasoning)?;
        Ok(())
    }
}
