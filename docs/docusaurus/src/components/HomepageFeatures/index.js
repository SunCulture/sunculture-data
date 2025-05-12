import clsx from 'clsx';
import Heading from '@theme/Heading';
import styles from './styles.module.css';

const FeatureList = [
  {
    title: "Data Catalog",
    Svg: require("@site/static/img/undraw_docusaurus_mountain.svg").default,
    description: (
      <>
        A centralized space to define KPIs, metrics, and data sources across
        departments— enabling transparency, alignment, and accurate reporting
        for teams.
      </>
    ),
  },
  {
    title: "Insights",
    Svg: require("@site/static/img/undraw_docusaurus_tree.svg").default,
    description: (
      <>
        Strategic trends, behaviors, and performance highlights derived from the
        data—supporting decision-making across Sales, Credit, Product, and Ops.
      </>
    ),
  },
  {
    title: "Releases",
    Svg: require("@site/static/img/undraw_docusaurus_react.svg").default,
    description: (
      <>
        Track updates to KPIs, dashboards, and data models over time—ensuring
        everyone stays informed on what's changed, when, and why.
      </>
    ),
  },
];

function Feature({Svg, title, description}) {
  return (
    <div className={clsx('col col--4')}>
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
