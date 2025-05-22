import clsx from "clsx";
import Heading from "@theme/Heading";
import styles from "./styles.module.css";

const FeatureList = [
  {
    title: "KPIs & Metrics",
    Svg: require("@site/static/img/undraw_docusaurus_mountain.svg").default,
    description: (
      <>
        Your single source of truth for business metrics. Explore clearly
        defined KPIs, consistent formulas, and trusted data sources—built to
        drive alignment, transparency, and confident reporting across
        SunCulture.
      </>
    ),
  },
  {
    title: "Dashboards",
    Svg: require("@site/static/img/undraw_docusaurus_tree.svg").default,
    description: (
      <>
        Interactive dashboards at your fingertips—surfacing real-time trends,
        team performance, and key business outcomes across Sales, Credit,
        Product, and Ops.
      </>
    ),
  },
  {
    title: "Insights",
    Svg: require("@site/static/img/undraw_docusaurus_tree.svg").default,
    description: (
      <>
        One home for all your dashboards. Find what exists, what it answers, who
        it serves, and where to view it—all in one organized, go-to space for
        data visibility.
      </>
    ),
  },
];

function Feature({ Svg, title, description }) {
  return (
    <div className={clsx("col col--4")}>
      <div className="text--center">
        <Svg className={styles.featureSvg} role="img" />
      </div>
      <div className="text--center padding-horiz--md">
        <Heading as="h3">{title}</Heading>
        <p>{description}</p>
      </div>
    </div>
  );
}

export default function HomepageFeatures() {
  return (
    <section className={styles.features}>
      <div className="container">
        <div className="row">
          {FeatureList.map((props, idx) => (
            <Feature key={idx} {...props} />
          ))}
        </div>
      </div>
    </section>
  );
}
