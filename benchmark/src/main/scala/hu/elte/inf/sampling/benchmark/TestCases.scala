package hu.elte.inf.sampling.benchmark

import hu.elte.inf.sampling.core.{AlgorithmConfiguration, SamplerCase, SamplingBase}
import hu.elte.inf.sampling.sampler.frequent.Frequent
import hu.elte.inf.sampling.sampler.landmark.LandmarkSampling
import hu.elte.inf.sampling.sampler.lossycounting.LossyCounting
import hu.elte.inf.sampling.sampler.spacesaving.SpaceSaving
import hu.elte.inf.sampling.sampler.stickysampling.StickySampling
import hu.elte.inf.sampling.sampler.{CheckpointSmoothed, TemporalSmoothed}

object TestCases {
  lazy val basicTestCases: (String, Seq[Int => SamplerCase[SamplingBase, AlgorithmConfiguration]]) = ("basicTestCases", stateOfTheArt._2 ++ Seq(
    (np: Int) => SamplerCase.fromFactory[TemporalSmoothed[LossyCounting, LossyCounting.Config], TemporalSmoothed.Config[LossyCounting, LossyCounting.Config]](
      TemporalSmoothed.Config(SamplerCase.fromFactory[LossyCounting, LossyCounting.Config](
        LossyCounting.Config(), new LossyCounting(_: LossyCounting.Config)),
        20000,
        20000
      ),
      TemporalSmoothed.apply,
      np
    ),
    (np: Int) => SamplerCase.fromFactory[TemporalSmoothed[LossyCounting, LossyCounting.Config], TemporalSmoothed.Config[LossyCounting, LossyCounting.Config]](
      TemporalSmoothed.Config(SamplerCase.fromFactory[LossyCounting, LossyCounting.Config](
        LossyCounting.Config(), new LossyCounting(_: LossyCounting.Config)),
        10000,
        10000
      ),
      TemporalSmoothed.apply,
      np
    ),
    (np: Int) => SamplerCase.fromFactory[TemporalSmoothed[LossyCounting, LossyCounting.Config], TemporalSmoothed.Config[LossyCounting, LossyCounting.Config]](
      TemporalSmoothed.Config(SamplerCase.fromFactory[LossyCounting, LossyCounting.Config](
        LossyCounting.Config(), new LossyCounting(_: LossyCounting.Config)),
        5000,
        5000
      ),
      TemporalSmoothed.apply,
      np
    ),
    (np: Int) => SamplerCase.fromFactory[CheckpointSmoothed[LossyCounting, LossyCounting.Config], CheckpointSmoothed.Config[LossyCounting, LossyCounting.Config]](
      CheckpointSmoothed.Config(SamplerCase.fromFactory[LossyCounting, LossyCounting.Config](
        LossyCounting.Config(), new LossyCounting(_: LossyCounting.Config)),
        20000,
        20000,
        0.2
      ),
      CheckpointSmoothed.apply,
      np
    ),
    (np: Int) => SamplerCase.fromFactory[CheckpointSmoothed[LossyCounting, LossyCounting.Config], CheckpointSmoothed.Config[LossyCounting, LossyCounting.Config]](
      CheckpointSmoothed.Config(SamplerCase.fromFactory[LossyCounting, LossyCounting.Config](
        LossyCounting.Config(), new LossyCounting(_: LossyCounting.Config)),
        10000,
        10000,
        0.2
      ),
      CheckpointSmoothed.apply,
      np
    ),
    (np: Int) => SamplerCase.fromFactory[CheckpointSmoothed[LossyCounting, LossyCounting.Config], CheckpointSmoothed.Config[LossyCounting, LossyCounting.Config]](
      CheckpointSmoothed.Config(SamplerCase.fromFactory[LossyCounting, LossyCounting.Config](
        LossyCounting.Config(), new LossyCounting(_: LossyCounting.Config)),
        5000,
        5000,
        0.2
      ),
      CheckpointSmoothed.apply,
      np
    ),
  ))

  lazy val monster = ("monster", Seq(
    (np: Int) => SamplerCase.fromFactory[CheckpointSmoothed[Frequent, Frequent.Config], CheckpointSmoothed.Config[Frequent, Frequent.Config]](
      CheckpointSmoothed.Config(SamplerCase.fromFactory[Frequent, Frequent.Config](Frequent.Config(200, 20000, 2000), new Frequent(_: Frequent.Config)),
        20000,
        20000,
        0.1
      ),
      CheckpointSmoothed.apply,
      np
    ),
    (np: Int) => SamplerCase.fromFactory[CheckpointSmoothed[Frequent, Frequent.Config], CheckpointSmoothed.Config[Frequent, Frequent.Config]](
      CheckpointSmoothed.Config(SamplerCase.fromFactory[Frequent, Frequent.Config](Frequent.Config(200, 40000, 5000), new Frequent(_: Frequent.Config)),
        80000,
        70000,
        0.1
      ),
      CheckpointSmoothed.apply,
      np
    ),
  ))
  lazy val cps_vaganza: (String, Seq[Int => SamplerCase[SamplingBase, AlgorithmConfiguration]]) = ("cps", Seq(
    (np: Int) => SamplerCase.fromFactory[Frequent, Frequent.Config](Frequent.Config(200, 40000, 5000), new Frequent(_: Frequent.Config), np),
    (np: Int) => SamplerCase.fromFactory[CheckpointSmoothed[LossyCounting, LossyCounting.Config], CheckpointSmoothed.Config[LossyCounting, LossyCounting.Config]](
      CheckpointSmoothed.Config(SamplerCase.fromFactory[LossyCounting, LossyCounting.Config](
        LossyCounting.Config(), new LossyCounting(_: LossyCounting.Config)),
        1,
        50000,
        0.5
      ),
      CheckpointSmoothed.apply,
      np
    ),
    (np: Int) => SamplerCase.fromFactory[CheckpointSmoothed[LossyCounting, LossyCounting.Config], CheckpointSmoothed.Config[LossyCounting, LossyCounting.Config]](
      CheckpointSmoothed.Config(SamplerCase.fromFactory[LossyCounting, LossyCounting.Config](
        LossyCounting.Config(), new LossyCounting(_: LossyCounting.Config)),
        1,
        100000,
        0.5
      ),
      CheckpointSmoothed.apply,
      np
    ),
    (np: Int) => SamplerCase.fromFactory[CheckpointSmoothed[LossyCounting, LossyCounting.Config], CheckpointSmoothed.Config[LossyCounting, LossyCounting.Config]](
      CheckpointSmoothed.Config(SamplerCase.fromFactory[LossyCounting, LossyCounting.Config](
        LossyCounting.Config(), new LossyCounting(_: LossyCounting.Config)),
        20000,
        20000,
        0.5
      ),
      CheckpointSmoothed.apply,
      np
    ),
    (np: Int) => SamplerCase.fromFactory[CheckpointSmoothed[Frequent, Frequent.Config], CheckpointSmoothed.Config[Frequent, Frequent.Config]](
      CheckpointSmoothed.Config(SamplerCase.fromFactory[Frequent, Frequent.Config](
        Frequent.Config(200, 40000, 5000), new Frequent(_: Frequent.Config)),
        1,
        50000,
        0.5
      ),
      CheckpointSmoothed.apply,
      np
    ),
    (np: Int) => SamplerCase.fromFactory[CheckpointSmoothed[Frequent, Frequent.Config], CheckpointSmoothed.Config[Frequent, Frequent.Config]](
      CheckpointSmoothed.Config(SamplerCase.fromFactory[Frequent, Frequent.Config](
        Frequent.Config(200, 40000, 5000), new Frequent(_: Frequent.Config)),
        1,
        50000,
        0.5
      ),
      CheckpointSmoothed.apply,
      np
    ),
  ))

  lazy val v_bunch: (String, Seq[Int => SamplerCase[SamplingBase, AlgorithmConfiguration]]) = ("v_bunch", Seq(
    (np: Int) => SamplerCase.fromFactory[StickySampling, StickySampling.Config](StickySampling.Config(), new StickySampling(_: StickySampling.Config), np),
    (np: Int) => SamplerCase.fromFactory[LossyCounting, LossyCounting.Config](LossyCounting.Config(), new LossyCounting(_: LossyCounting.Config), np),
    (np: Int) => SamplerCase.fromFactory[Frequent, Frequent.Config](Frequent.Config(400, 40000, 5000), new Frequent(_: Frequent.Config), np),
    //   (np: Int) => SamplerCase.fromFactory[DriftRespectingV2, DriftRespectingV2.Config](DriftRespectingV2.Config(DriftRespectingV2.Exponential()), DriftRespectingV2.apply, np),
    //  (np: Int) => SamplerCase.fromFactory[DriftRespectingVGOD, DriftRespectingVGOD.Config](DriftRespectingVGOD.Config(), DriftRespectingVGOD.apply, np),
  ))

  lazy val stateOfTheArt = ("sota", Seq(
    (np: Int) => SamplerCase.fromFactory[StickySampling, StickySampling.Config](StickySampling.Config(), new StickySampling(_: StickySampling.Config), np),
    (np: Int) => SamplerCase.fromFactory[LossyCounting, LossyCounting.Config](LossyCounting.Config(), new LossyCounting(_: LossyCounting.Config), np),
    (np: Int) => SamplerCase.fromFactory[SpaceSaving, SpaceSaving.Config](SpaceSaving.Config(0.0001D), new SpaceSaving(_: SpaceSaving.Config), np),
    (np: Int) => SamplerCase.fromFactory[Frequent, Frequent.Config](Frequent.Config(400, 40000, 5000), new Frequent(_: Frequent.Config), np),
    (np: Int) => SamplerCase.fromFactory[LandmarkSampling, LandmarkSampling.Config](LandmarkSampling.Config(40000), LandmarkSampling.apply, np),
  ))
  lazy val stateOfTheArt_withoutlandmark = ("sotanoladnmark", Seq(
    (np: Int) => SamplerCase.fromFactory[StickySampling, StickySampling.Config](StickySampling.Config(), new StickySampling(_: StickySampling.Config), np),
    (np: Int) => SamplerCase.fromFactory[LossyCounting, LossyCounting.Config](LossyCounting.Config(), new LossyCounting(_: LossyCounting.Config), np),
    (np: Int) => SamplerCase.fromFactory[SpaceSaving, SpaceSaving.Config](SpaceSaving.Config(0.0001D), new SpaceSaving(_: SpaceSaving.Config), np),
    (np: Int) => SamplerCase.fromFactory[Frequent, Frequent.Config](Frequent.Config(400, 40000, 5000), new Frequent(_: Frequent.Config), np),
  ))
  lazy val oursoluzion = ("solution", Seq(
    (np: Int) => SamplerCase.fromFactory[Frequent, Frequent.Config](Frequent.Config(400, 40000, 5000), new Frequent(_: Frequent.Config), np),
    (np: Int) => SamplerCase.fromFactory[LossyCounting, LossyCounting.Config](LossyCounting.Config(), new LossyCounting(_: LossyCounting.Config), np),
    (np: Int) => SamplerCase.fromFactory[CheckpointSmoothed[LossyCounting, LossyCounting.Config], CheckpointSmoothed.Config[LossyCounting, LossyCounting.Config]](
      CheckpointSmoothed.Config(SamplerCase.fromFactory[LossyCounting, LossyCounting.Config](
        LossyCounting.Config(), new LossyCounting(_: LossyCounting.Config)),
        40000,
        40000,
        0.20
      ),
      CheckpointSmoothed.apply,
      np
    ),
    (np: Int) => SamplerCase.fromFactory[TemporalSmoothed[LossyCounting, LossyCounting.Config], TemporalSmoothed.Config[LossyCounting, LossyCounting.Config]](
      TemporalSmoothed.Config(SamplerCase.fromFactory[LossyCounting, LossyCounting.Config](
        LossyCounting.Config(), new LossyCounting(_: LossyCounting.Config)),
        40000,
        40000
      ),
      TemporalSmoothed.apply,
      np
    ),
  ))

  lazy val frequent = ("smartFrequent", stateOfTheArt._2 ++ Seq(
    (np: Int) => SamplerCase.fromFactory[Frequent, Frequent.Config](Frequent.Config(400, 40000, 5000), new Frequent(_: Frequent.Config), np)
  ))
  lazy val stateOfTheArt2 = ("sota2", Seq(
    (np: Int) => SamplerCase.fromFactory[StickySampling, StickySampling.Config](StickySampling.Config(), new StickySampling(_: StickySampling.Config), np),
    (np: Int) => SamplerCase.fromFactory[LossyCounting, LossyCounting.Config](LossyCounting.Config(), new LossyCounting(_: LossyCounting.Config), np),
    (np: Int) => SamplerCase.fromFactory[SpaceSaving, SpaceSaving.Config](SpaceSaving.Config(0.0001D), new SpaceSaving(_: SpaceSaving.Config), np),
    (np: Int) => SamplerCase.fromFactory[Frequent, Frequent.Config](Frequent.Config(200, 40000, 5000), new Frequent(_: Frequent.Config), np),
  ))

  lazy val lossy_example = ("lossy", Seq(
    (np: Int) => SamplerCase.fromFactory[LossyCounting, LossyCounting.Config](LossyCounting.Config(), new LossyCounting(_: LossyCounting.Config), np),
  ))

  lazy val sticky_example = ("sticky", Seq(
    (np: Int) => SamplerCase.fromFactory[StickySampling, StickySampling.Config](StickySampling.Config(), new StickySampling(_: StickySampling.Config), np),
  ))

}
