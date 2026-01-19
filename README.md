# TP4 — DataOps Pipeline (Scraping & Nettoyage)

## 🎯 Objectif
Ce projet met en place un pipeline **DataOps reproductible** permettant de :
- scraper plusieurs sources de données (Budget, Football, INPC),
- stocker les résultats sous forme de fichiers CSV,
- nettoyer et préparer les données INPC extraites d’un PDF,
- produire des indicateurs de qualité (KPI),
- orchestrer l’ensemble du pipeline via Docker.

---

## 🧱 Structure du projet---

## ⚙️ Technologies utilisées
- **Python 3.12**
- **pandas**
- **requests / BeautifulSoup**
- **Camelot** (extraction de tableaux PDF)
- **Playwright** (scraping dynamique)
- **Docker & Docker Compose**

---

## ▶️ Exécution du pipeline

### 1️⃣ Construction et lancement
```bash
docker compose up -d --build
