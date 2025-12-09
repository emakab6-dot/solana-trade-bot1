# Solana Dex Trades Bot (Helius + Telegram)

Bot Python qui écoute les pools Raydium/Orca via Helius (WebSocket),
filtre les gros trades (>= 1000 USD) et envoie des alertes dans un bot Telegram.

## Fichiers

- `main.py` : script principal du bot
- `requirements.txt` : dépendances Python
- `pools-raydium-orca.json` : **à remplacer par ton vrai fichier JSON de pools**

## Variables d'environnement

Avant de lancer le bot, définir les variables d'environnement :

```bash
export TELEGRAM_TOKEN="xxx"
export HELIUS_API_KEY="xxx"
```

Sur Render, configure ces variables dans l'onglet **Environment** du Background Worker.

## Lancement en local

```bash
pip install -r requirements.txt
python main.py
```

## Déploiement sur Render (Background Worker)

1. Crée un repo GitHub avec ces fichiers.
2. Connecte ton GitHub à Render.
3. Crée un **Background Worker**.
4. Build command :

   ```bash
   pip install -r requirements.txt
   ```

5. Start command :

   ```bash
   python main.py
   ```

6. Ajoute les variables d'environnement `TELEGRAM_TOKEN` et `HELIUS_API_KEY` dans Render.
7. Remplace `pools-raydium-orca.json` par ton vrai fichier JSON avant de pousser sur GitHub.
