# Painel de Conjuntura 2.0

Frontend em Next.js com visualizações ECharts. Os dados são exportados dos
parquets da raiz do projeto, mantidos pela branch `main`.

```powershell
python scripts/export_data.py
npm install
npm run dev
```

Para validar a versão de produção:

```powershell
npm run build
```
