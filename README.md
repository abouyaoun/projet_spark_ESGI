# Spark Hands on

* [Exercice 0](exo0.md)
* [Exercice 1](exo1.md)
* [Exercice 2](exo2.md)
* [Exercice 3](exo3.md)
* [Exercice 4](exo4.md)
* [Exercice 5](exo5.md)

---

# Résultats de Performance des UDFs dans Spark (exo 4)

## **Partie UDF :**

### **Temps d'exécution :**
- **Sans UDF (Spark natives)** : `0.16 secondes`
- **UDF Scala** : `0.24 secondes`
- **UDF Python** : `0.60 secondes`

### **Analyse des performances :**
1. **Sans UDF (Spark natives)** :
    - 🏆 **La méthode la plus rapide**.
    - Utilise les optimisations internes de Spark pour des performances maximales.
    
2. **UDF Scala** :
    - Légèrement plus lente que les fonctions natives.
    - Beaucoup plus performante qu’une UDF Python grâce à son exécution directe dans la JVM.

3. **UDF Python** :
    - La méthode **la plus lente**.
    - Nécessite une sérialisation et désérialisation entre Python et JVM, ce qui introduit un coût supplémentaire.

---
