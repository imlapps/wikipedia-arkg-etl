from dagster import define_asset_job

from .assets import (
    wikipedia_anti_recommendations,
    wikipedia_arkg_assets,
    wikipedia_articles_vector_store,
)

embedding_job = define_asset_job(
    "embedding_job", selection=["*" + wikipedia_articles_vector_store.key.path[0]]
)
retrieval_job = define_asset_job(
    "retrieval_job",
    selection=["*" + wikipedia_anti_recommendations.key.path[0]],
)
arkg_job = define_asset_job("arkg_job", selection=wikipedia_arkg_assets)
